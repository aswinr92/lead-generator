"""
Quick script to open Google Maps and pause for inspection
"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

# Setup Chrome
chrome_options = Options()
chrome_options.add_argument('--disable-blink-features=AutomationControlled')

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

try:
    # Open Google Maps search
    query = "wedding caterers Trivandrum Kerala"
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    
    print(f"🌐 Opening: {search_url}")
    driver.get(search_url)
    
    # Wait for page to load
    time.sleep(5)
    
    print("\n" + "="*60)
    print("🛑 BROWSER IS OPEN - INSPECT THE PAGE NOW")
    print("="*60)
    print("\nLook for:")
    print("  1. The scrollable results panel")
    print("  2. Individual vendor cards/links")
    print("  3. Right-click → Inspect to see HTML")
    print("\nPress ENTER when done inspecting...")
    print("="*60)
    
    input()  # 🛑 Pauses here
    
    print("\n✅ Continuing...")
    
    # You can add test code here to try selectors
    from selenium.webdriver.common.by import By
    
    # Try to find elements
    try:
        feed = driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
        print("✅ Found div[role='feed']")
    except:
        print("❌ div[role='feed'] not found")
    
    try:
        results = driver.find_elements(By.CSS_SELECTOR, "div[role='article']")
        print(f"✅ Found {len(results)} div[role='article'] elements")
    except:
        print("❌ div[role='article'] not found")
    
    # Add more selector tests here
    
    input("\nPress ENTER to close browser...")
    
finally:
    driver.quit()
    print("✅ Browser closed")