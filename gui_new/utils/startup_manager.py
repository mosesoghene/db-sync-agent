import os
import sys
import winreg

class StartupManager:
    def __init__(self):
        self.app_name = "DB Sync Agent"
        self.key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"

    def add_to_startup(self):
        try:
            # Get the path to the executable
            if getattr(sys, 'frozen', False):
                # If the application is run as a bundle
                app_path = sys.executable
            else:
                # If the application is run from a Python interpreter
                app_path = os.path.abspath(sys.argv[0])

            # Open the registry key
            key = winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                self.key_path,
                0,
                winreg.KEY_ALL_ACCESS
            )

            # Set the registry value
            winreg.SetValueEx(
                key,
                self.app_name,
                0,
                winreg.REG_SZ,
                f'"{app_path}"'
            )

            # Close the key
            winreg.CloseKey(key)
            return True
        except Exception as e:
            print(f"Error adding to startup: {str(e)}")
            return False

    def remove_from_startup(self):
        try:
            # Open the registry key
            key = winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                self.key_path,
                0,
                winreg.KEY_ALL_ACCESS
            )

            # Delete the registry value
            winreg.DeleteValue(key, self.app_name)

            # Close the key
            winreg.CloseKey(key)
            return True
        except Exception as e:
            print(f"Error removing from startup: {str(e)}")
            return False

    def is_in_startup(self):
        try:
            # Open the registry key
            key = winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                self.key_path,
                0,
                winreg.KEY_READ
            )

            # Try to read the value
            winreg.QueryValueEx(key, self.app_name)
            winreg.CloseKey(key)
            return True
        except:
            return False
