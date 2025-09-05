# Create a file: user_management/middleware.py or create a new file like security/middleware.py

import re
import logging
import json
from django.http import HttpResponseBadRequest
from django.http.request import RawPostDataException
from django.utils.deprecation import MiddlewareMixin

logger = logging.getLogger(__name__)

class SQLInjectionProtectionMiddleware(MiddlewareMixin):
    """
    Middleware to detect and block potential SQL injection attempts
    """
    
    # Common SQL injection patterns
    SQL_INJECTION_PATTERNS = [
        r"(\%27)|(\')|(\-\-)|(\%23)|(#)",  # Single quotes, comments
        r"((\%3D)|(=))[^\n]*((\%27)|(\')|(\-\-)|(\%23)|(#))",  # Equals with quotes/comments
        r"w*((\%27)|(\')|(\-\-)|(\%23)|(#))",  # General patterns
        r"((\%27)|(\')|(\-\-)|(\%23)|(#))union",  # UNION attacks
        r"union(.*?)select",  # UNION SELECT
        r"select(.*?)from",  # SELECT FROM
        r"insert(.*?)into",  # INSERT INTO
        r"delete(.*?)from",  # DELETE FROM
        r"update(.*?)set",  # UPDATE SET
        r"drop(.*?)table",  # DROP TABLE
        r"create(.*?)table",  # CREATE TABLE
        r"exec(.*?)\s",  # EXEC commands
        r"script",  # Script tags
        r"javascript:",  # JavaScript
        r"vbscript:",  # VBScript
        r"onload",  # Event handlers
        r"onerror",
        r"onclick",
        r"'(\s*)or(\s*)('|\d)",  # OR injection patterns
        r"'(\s*)and(\s*)('|\d)",  # AND injection patterns  
        r"'(\s*)=(\s*)'",  # Equality checks
        r"1(\s*)=(\s*)1",  # Common tautology
        r"0(\s*)=(\s*)0",  # Common tautology
        r"true(\s*)=(\s*)true",  # Boolean tautology
        r"false(\s*)=(\s*)false",  # Boolean tautology
        r"null(\s*)=(\s*)null",  # Null comparison
        r";(\s*)(drop|delete|insert|update|create)",  # Semicolon attacks
    ]
    
    def __init__(self, get_response):
        self.get_response = get_response
        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.SQL_INJECTION_PATTERNS]
    
    def __call__(self, request):
        # Check request before processing
        if self.contains_sql_injection(request):
            logger.warning(f"SQL injection attempt detected from {request.META.get('REMOTE_ADDR')}: {request.get_full_path()}")
            return HttpResponseBadRequest("Invalid request detected")
        
        response = self.get_response(request)
        return response
    
    def contains_sql_injection(self, request):
        """
        Check if request contains potential SQL injection
        """
        # Normalize content type (ignore charset, etc.)
        content_type = (request.content_type or '').split(';')[0].lower()

        # Check GET parameters
        for key, value in request.GET.items():
            if self.is_malicious(value):
                logger.warning(f"SQL injection in GET parameter '{key}': {value}")
                return True

        # Only inspect one source based on content type to avoid consuming the stream twice
        try:
            if content_type == 'application/json':
                # Safely inspect JSON body without touching request.POST
                try:
                    body_bytes = request.body  # May raise RawPostDataException if already consumed
                    json_data = json.loads(body_bytes.decode('utf-8'))
                    if self.check_json_data(json_data):
                        return True
                except RawPostDataException:
                    # Body already consumed (e.g., by previous middleware) — skip JSON inspection
                    logger.debug("Skipping JSON body inspection: raw post data already consumed")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # If we can't parse JSON, check the raw body (best-effort)
                    try:
                        raw = body_bytes.decode('utf-8', errors='ignore')
                        if self.is_malicious(raw):
                            logger.warning(f"SQL injection in request body: {raw}")
                            return True
                    except Exception:
                        pass

            elif content_type.startswith('multipart/'):
                # File uploads: never touch request.body; only inspect form fields
                for key, value in request.POST.items():
                    if self.is_malicious(value):
                        logger.warning(f"SQL injection in multipart POST parameter '{key}': {value}")
                        return True

            elif content_type in ('application/x-www-form-urlencoded', 'text/plain'):
                # Regular forms: POST is safe to read; avoid body access
                for key, value in request.POST.items():
                    if self.is_malicious(value):
                        logger.warning(f"SQL injection in POST parameter '{key}': {value}")
                        return True
            else:
                # Fallback: attempt to read body if available and not consumed
                try:
                    raw = request.body.decode('utf-8', errors='ignore')
                    if raw and self.is_malicious(raw):
                        logger.warning(f"SQL injection in raw request body: {raw}")
                        return True
                except RawPostDataException:
                    logger.debug("Skipping raw body inspection: raw post data already consumed")
        except Exception:
            # Be conservative; do not block the request on middleware inspection errors
            logger.debug("SQL injection inspection encountered a non-fatal error", exc_info=True)
        
        # Check path (be more lenient with paths)
        if self.is_malicious_path(request.path):
            logger.warning(f"SQL injection in request path: {request.path}")
            return True
            
        return False
    
    def check_json_data(self, data):
        """
        Recursively check JSON data for SQL injection patterns
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if self.is_malicious(str(key)) or self.check_json_data(value):
                    logger.warning(f"SQL injection in JSON key '{key}' or value: {value}")
                    return True
        elif isinstance(data, list):
            for item in data:
                if self.check_json_data(item):
                    return True
        elif isinstance(data, str):
            if self.is_malicious(data):
                logger.warning(f"SQL injection in JSON string: {data}")
                return True
        return False
    
    def is_malicious_path(self, path):
        """
        Check if path contains SQL injection patterns (more restrictive for paths)
        """
        dangerous_path_patterns = [
            r"union(.*?)select",
            r"drop(.*?)table",
            r"exec(.*?)\s",
            r"delete(.*?)from",
            r"insert(.*?)into",
        ]
        
        for pattern_str in dangerous_path_patterns:
            pattern = re.compile(pattern_str, re.IGNORECASE)
            if pattern.search(path):
                return True
        return False
    
    def is_malicious(self, value):
        """
        Check if a value contains SQL injection patterns
        """
        if not isinstance(value, str):
            value = str(value)
            
        for pattern in self.compiled_patterns:
            if pattern.search(value):
                return True
        return False