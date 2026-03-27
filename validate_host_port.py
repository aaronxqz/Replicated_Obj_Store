import socket
import re

def validate_host_port(input_string):
    """
    Validates a string for the format 'host:port' where host can be 'localhost' 
    or a normal IP address, and the port is within the valid range.
    """
    # Regex to match the general format: host/ip followed by a colon and numbers
    # The host part accepts 'localhost' or an IP pattern (but doesn't fully validate IP format yet)
    pattern = re.compile(r"^(localhost|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d+)$")
    
    match = pattern.match(input_string)
    if not match:
        return False, "Invalid format. Expected 'host:port'."

    host, port_str = match.groups()

    # 1. Validate the port number range (0 to 65535)
    try:
        port = int(port_str)
        if not (0 <= port <= 65535):
            return False, f"Port number {port} is out of the valid range (0-65535)."
    except ValueError:
        # Should be caught by regex, but as a safeguard
        return False, "Invalid port number format."

    # 2. Validate the host part (if it's not 'localhost', it must be a valid IP)
    if host.lower() != 'localhost':
        try:
            # socket.inet_aton() raises an error for invalid IP addresses
            socket.inet_aton(host)
        except socket.error:
            return False, f"'{host}' is not a valid IP address."
    
    return True, f"Valid host and port: {host}:{port}"
'''
# --- Example Usage ---
test_strings = [
    "localhost:8080",
    "127.0.0.1:443",
    "192.168.1.100:65535",
    "invalid_host:80",
    "10.0.0.1:99999",
    "192.168.1:80",
    "localhost:invalid",
    "localhost:w"
]

for s in test_strings:
    is_valid, message = validate_host_port(s)
    print(f"'{s}': {'Valid' if is_valid else 'Invalid'} - {message}")
'''