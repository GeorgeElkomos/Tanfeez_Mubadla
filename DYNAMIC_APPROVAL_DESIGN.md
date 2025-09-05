# Dynamic Approval Process Design

Goal: Replace the current fixed 4-column approval tracking on `xx_BudgetTransfer` (approvel_1..4, approvel_*_date, status_level) with a flexible, data‑driven approval engine supporting:
- Variable number of stages per transfer type (FAR / AFR / FAD / future types)
- Parallel or conditional stages (optional future extension)
- Per stage multiple approvers (all must approve OR any can approve)
- Auditable history (who did what, when, decision & comment)
- Reconfiguration without schema change

## Current State Pain Points
- Hard-coded fields: `approvel_1..4`, `status_level` limit you to 4 linear steps.
- Logic duplicated in views (increment `status_level`, set different fields) → harder to extend.
- No normalized approval action history (reject reasons separate, approvals embedded).
- Changing number/order of levels requires migration & code edits.

## Target Architecture

### Core Concepts
1. Workflow Template (definition) – describes the ordered (or conditional) stages for a transfer type.
2. Workflow Stage – a single approval step with rules (sequence order, required approver roles/levels, decision policy: ALL / ANY / QUORUM, SLA, auto‑escalation).
3. Workflow Instance – runtime link between a Budget Transfer and a Workflow Template; tracks current stage pointer & overall state.
4. Stage Assignment – which users (or dynamic user query) are eligible to act in a particular instance stage.
5. Approval Action (History) – individual user decision records (approve/reject/reassign/comment).
6. Stage Aggregator – logic to evaluate when a stage is complete (based on actions vs policy) and advance or finalize.

### Proposed Models (New)

`ApprovalWorkflowTemplate`
- id (PK)
- code (e.g. FAR_DEFAULT) unique
- transfer_type (FAR/AFR/FAD/...)
- name, description
- is_active (bool)
- version (int) – allow deprecating old definitions

`ApprovalWorkflowStageTemplate`
- id (PK)
- workflow_template (FK → ApprovalWorkflowTemplate, related_name="stages")
- order_index (int) (sequence)
- name (e.g. "Finance Level 1")
- decision_policy (enum: ALL, ANY, QUORUM)
- quorum_count (nullable int) – used when policy=QUORUM
- required_user_level (nullable FK → xx_UserLevel) OR
- required_role (nullable char) OR
- dynamic_filter (JSON, optional future: e.g. {"ability.Type": "approve", "entity_scope":"transfer.entities"})
- allow_reject (bool)
- allow_delegate (bool)
- sla_hours (int nullable)

`ApprovalWorkflowInstance`
- id (PK)
- budget_transfer (FK → xx_BudgetTransfer, unique)  (One active instance per transfer)
- template (FK → ApprovalWorkflowTemplate)
- current_stage_template (FK → ApprovalWorkflowStageTemplate, nullable when finished)
- status (enum: pending, in_progress, approved, rejected, cancelled)
- started_at, finished_at

`ApprovalWorkflowStageInstance`
- id
- workflow_instance (FK → ApprovalWorkflowInstance, related_name="stage_instances")
- stage_template (FK → ApprovalWorkflowStageTemplate)
- status (pending, active, completed, skipped, cancelled)
- activated_at, completed_at

`ApprovalAssignment`
- id
- stage_instance (FK)
- user (FK → xx_User)  (materialized list of eligible approvers)
- role_snapshot / level_snapshot (denormalize for audit)
- is_mandatory (bool)  (if you want sub-group logic)

`ApprovalAction`
- id
- stage_instance (FK)
- user (FK → xx_User)
- action (approve, reject, delegate, comment)
- comment (Text)
- created_at
- triggers_stage_completion (bool) (denormalize for reporting)

`ApprovalDelegation` (Optional future)
- id
- from_user (FK) → delegated_by
- to_user (FK)
- stage_instance (FK)
- active (bool)

### Modifications to Existing Models
`xx_BudgetTransfer`
- Deprecate (retire gradually): approvel_1..4, approvel_X_date, status_level
- Keep `status` but drive its state from the workflow instance (synchronize on transitions)
- Possibly add: `workflow_status_cache` (char) for reporting index

### Workflow Resolution Flow
1. On creating a `xx_BudgetTransfer`, select the active `ApprovalWorkflowTemplate` by `transfer_type`.
2. Instantiate `ApprovalWorkflowInstance` & preload stage instances (or create lazily when entering stage).
3. Activate first stage: generate assignments -> all users matching `required_user_level` OR `required_role` OR dynamic filter (abilities). Store snapshots.
4. User performs `ApprovalAction` (approve/reject/comment). After each action evaluate stage completion:
   - ALL: every assignment has action=approve → stage complete
   - ANY: first approve completes; reject may either stop or await others (config) – define a `reject_policy` if needed
   - QUORUM: number of approves >= quorum_count
5. On reject (with allow_reject): mark instance status=rejected, finalize; can optionally auto-create reopen logic.
6. On stage completion: set `completed_at`, advance to next stage; if none, set workflow + transfer status approved.
7. Persist audit trail in `ApprovalAction` rows.

### Rejection Handling
Approach: Replace separate `xx_BudgetTransferRejectReason` with `ApprovalAction` where action=reject, comment=reason. (Optionally keep existing table temporarily & populate both until migration done.)

### Migration Strategy
Phase 1 (Coexist):
- Create new workflow tables; backfill a "LEGACY_4_STEP" template matching current linear process.
- Write a management command to populate template & stage templates (4 stages for FAR/AFR with order_index 1..4; 3 for FAD if needed).
- For existing transfers: create workflow instances pointing to appropriate stage based on `status_level` and fill synthetic actions for already-approved levels using existing approvel_* fields.

Phase 2 (Switch Writes):
- Update view logic to stop writing approvel_* fields; instead create `ApprovalAction` and transition stages.
- Maintain backward compatibility by still reading old fields for legacy records until fully migrated.

Phase 3 (Cleanup):
- Drop deprecated columns after verification period.
- Remove legacy status_level logic from views.

### Decision Policies & Edge Cases
- Mixed Role + Level: if both specified treat as AND; else dynamic_filter can express advanced logic.
- Escalation: Add async task scanning for active stage instances where now() > activated_at + sla_hours → notify/escalate.
- Delegation: if `delegate` action allowed, create `ApprovalDelegation` and optionally add new assignment for target user.
- Parallel Stages (future): add `parallel_group` column to StageTemplate — all in a group activate simultaneously; workflow completes group when all group stages completed. For now keep simple sequential.

### Permissions Mapping
Existing `xx_UserLevel.level_order` can map to stage requirements. A stage template referencing a level requires any (or ALL if specified) users with that level. Optionally produce assignments only for users with ability Type='approve' for the relevant entities (extend dynamic_filter evaluation).

### API Adjustments
New endpoints (conceptual):
- GET /api/budget/workflows/templates/ (list templates)
- POST /api/budget/workflows/{template_id}/activate (enable/disable)
- GET /api/budget/transfers/{id}/workflow/ (instance details: stages, assignments, actions)
- POST /api/budget/transfers/{id}/workflow/stages/{stage_instance_id}/actions/ { action: approve|reject|comment|delegate, comment?, delegate_to? }

### Minimal Changes to Existing Views
- In create transfer view: after saving transfer, call service `workflow_engine.start_for_transfer(transfer)`.
- In approval/reject view: replace existing level arithmetic with call `workflow_engine.record_action(transfer, user, action, comment)`.
- When retrieving a transfer: include aggregated workflow status (current stage name, progress % = completed_stages / total_stages).

### Service Layer (Recommended Modules)
Create `budget_management/workflow/` package:
- `engine.py` – high-level API: start_for_transfer, record_action, advance_if_complete
- `assignment.py` – logic to resolve eligible users
- `policies.py` – implement ALL/ANY/QUORUM evaluation
- `signals.py` – Django signals hooking into transfer creation

### Pseudocode Snippet (Conceptual)
```
# engine.record_action
with transaction.atomic():
  action = ApprovalAction.objects.create(...)
  stage = action.stage_instance
  if policies.is_stage_complete(stage):
      stage.status = 'completed'; stage.completed_at = now(); stage.save()
      next_stage = engine.get_next_stage(stage.workflow_instance)
      if next_stage:
          engine.activate_stage(next_stage)
      else:
          wf = stage.workflow_instance
          wf.status = 'approved'; wf.finished_at = now(); wf.save()
          transfer.status = 'approved'; transfer.save(update_fields=['status'])
```

### Reporting & Performance
- For list screens requiring current approval level, index `ApprovalWorkflowInstance(status, current_stage_template_id)`.
- To compute progress fast, store `completed_stage_count` on instance (denormalize) updated atomically.

### Data Integrity Constraints
- Unique(workflow_template, order_index)
- One active workflow template per (transfer_type, version) pair.
- Only one active stage_instance per workflow_instance at a time (for sequential implementation).

### Migration Commands (Outline)
1. `python manage.py create_default_workflows`
   - Creates FAR_DEFAULT, AFR_DEFAULT, FAD_DEFAULT templates & stages
2. `python manage.py backfill_workflow_instances` (idempotent)
   - For each transfer without instance create and sync stage/actions from legacy fields

### Rollback Plan
- Keep legacy columns populated until after verifying new workflow for N days.
- Feature flag (settings.USE_DYNAMIC_APPROVAL) gating new engine.

### Risks & Mitigations
| Risk | Mitigation |
|------|------------|
| Partial migration inconsistency | Transactions around creation & actions; idempotent backfill |
| Performance on assignment resolution | Cache user lists per stage template + invalidation on user change |
| Complex conditional logic creep | Start simple (sequential), design extensible fields (parallel_group, dynamic_filter) |

### Future Extensions
- Conditional branching: add `condition_expression` on stage templates evaluated against transfer attributes.
- Parallel approvals: `parallel_group` numeric key.
- SLA escalations & reminders: periodic Celery beat task scanning active stages.
- UI progress timeline endpoint.

### Summary
This structure decouples approval logic from the budget transfer model, enabling unlimited, configurable approval flows, richer auditing, and simpler future enhancements while allowing a phased migration that preserves existing data until stable.
