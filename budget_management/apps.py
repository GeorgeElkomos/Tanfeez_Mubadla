from django.apps import AppConfig

class BudgetManagementConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'budget_management'
    verbose_name = 'Budget Management'

    def ready(self):
        """
        Initialize signals when the app is ready
        This ensures signals are registered when Django starts
        """
        try:
            # Import budget transfer signals to register them
            from .signals import budget_trasnfer
            print("Budget management signals registered successfully")
        except ImportError as e:
            print(f"Error importing budget management signals: {e}")
        except Exception as e:
            print(f"Error registering budget management signals: {e}")
