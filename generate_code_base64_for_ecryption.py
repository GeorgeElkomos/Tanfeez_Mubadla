import base64
import os
 
# Generate a proper Fernet key
key = base64.urlsafe_b64encode(os.urandom(32)).decode()
print(key)