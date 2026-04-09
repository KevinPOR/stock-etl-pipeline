import pyautogui
import time
import subprocess

# Path to PBIX
pbix_path = r"D:\Data-Engineering-Project\Dashboard_for_Automation.pbix"
powerbi_path = r"D:\Microsoft Power BI Desktop\bin\PBIDesktop.exe"

# Open Power BI
subprocess.Popen([powerbi_path, pbix_path])

# Wait for it to fully open
time.sleep(25)

# Trigger refresh (Ctrl + R)
pyautogui.hotkey('ctrl', 'r')

# Wait for refresh to complete
time.sleep(120)

# Close Power BI
pyautogui.hotkey('alt', 'f4')