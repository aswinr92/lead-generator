"""
Google Maps Scraper for Wedding Vendors
Scrapes vendor information from Google Maps based on cities and categories.
"""

import time
import random
import re
from datetime import datetime
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent


class GoogleMapsVendorScraper:
    """Scraper for extracting wedding vendor data from Google Maps."""

    def __init__(self, headless: bool = False, implicit_wait: int = 10):
        """
        Initialize the scraper with Chrome WebDriver.

        Args:
            headless: Run browser in headless mode
            implicit_wait: Selenium implicit wait time in seconds
        """
        self.driver = self._setup_driver(headless)
        self.driver.implicitly_wait(implicit_wait)
        self.wait = WebDriverWait(self.driver, 10)

    def _setup_driver(self, headless: bool) -> webdriver.Chrome:
        """Set up Chrome WebDriver with appropriate options."""
        chrome_options = Options()

        if headless:
            chrome_options.add_argument('--headless')

        # Anti-detection measures
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Random user agent
        ua = UserAgent()
        chrome_options.add_argument(f'user-agent={ua.random}')

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # Additional anti-detection
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return driver

    def search_vendors(self, query: str, max_results: int = 50) -> List[Dict]:
        """
        Search for vendors on Google Maps.

        Args:
            query: Search query (e.g., "wedding caterers in Kochi")
            max_results: Maximum number of results to scrape

        Returns:
            List of vendor dictionaries
        """
        print(f"\n🔍 Searching: {query}")

        # Construct Google Maps search URL
        search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        self.driver.get(search_url)

        # Wait for results to load
        time.sleep(3)
        # Scroll to load more results
        self._scroll_results_panel(max_results)

        # Extract vendor links
        vendor_links = self._extract_vendor_links()

        # Limit to max_results
        vendor_links = vendor_links[:max_results]

        print(f"📊 Found {len(vendor_links)} vendors")

        # Scrape each vendor's details
        vendors = []
        for idx, link in enumerate(vendor_links, 1):
            print(f"  [{idx}/{len(vendor_links)}] Scraping vendor...")
            vendor_data = self._scrape_vendor_details(link, query)
            if vendor_data:
                vendors.append(vendor_data)

            # Rate limiting
            time.sleep(random.uniform(1, 2))

        return vendors

    def _scroll_results_panel(self, max_results: int, scroll_pause: float = 2.0):
        """Scroll the Google Maps results panel to load more results."""
        try:
            # Find the scrollable results panel
            scrollable_div = self.driver.find_element(
                By.CSS_SELECTOR,
                "div[role='feed']"
            )

            last_height = 0
            scroll_attempts = 0
            max_scroll_attempts = 20

            while scroll_attempts < max_scroll_attempts:
                # Scroll down
                self.driver.execute_script(
                    'arguments[0].scrollTo(0, arguments[0].scrollHeight);',
                    scrollable_div
                )

                time.sleep(scroll_pause)

                # Check if we've reached the end
                new_height = self.driver.execute_script(
                    "return arguments[0].scrollHeight",
                    scrollable_div
                )

                # Count current results
                results = self.driver.find_elements(By.CSS_SELECTOR, "div[role='feed'] > div > div > a")

                if new_height == last_height or len(results) >= max_results:
                    break

                last_height = new_height
                scroll_attempts += 1

        except Exception as e:
            print(f"⚠️  Scrolling warning: {str(e)}")

    def _extract_vendor_links(self) -> List[str]:
        """Extract vendor profile links from search results."""
        try:
            # Find all vendor links in the results panel
            link_elements = self.driver.find_elements(
                By.CSS_SELECTOR,
                "div[role='feed'] a[href*='/maps/place/']"
            )

            # Get unique links
            links = list(set([elem.get_attribute('href') for elem in link_elements if elem.get_attribute('href')]))

            return links

        except Exception as e:
            print(f"❌ Error extracting links: {str(e)}")
            return []

    def _scrape_vendor_details(self, url: str, search_query: str) -> Optional[Dict]:
        """
        Scrape detailed information from a vendor's Google Maps page.

        Args:
            url: Vendor's Google Maps URL
            search_query: Original search query for context

        Returns:
            Dictionary with vendor details
        """
        try:
            self.driver.get(url)
            time.sleep(2)

            # Extract name with multiple fallbacks
            name = self._extract_business_name()

            vendor = {
                'name': name,
                'category': self._extract_category(),
                'rating': self._extract_rating(),
                'reviews_count': self._extract_reviews_count(),
                'address': self._safe_extract_text("button[data-item-id='address']"),
                'phone': self._extract_phone(),
                'website': self._extract_website(),
                'url': url,
                'search_query': search_query,
                'scraped_at': datetime.now().isoformat()
            }

            # Skip record if critical fields are missing
            if not name and not vendor['phone']:
                print(f"    ⚠️  Skipping: Missing both name and phone")
                return None

            return vendor

        except Exception as e:
            print(f"    ⚠️  Error scraping vendor: {str(e)}")
            return None

    def _safe_extract_text(self, selector: str, by: By = By.CSS_SELECTOR) -> str:
        """Safely extract text from an element."""
        try:
            element = self.driver.find_element(by, selector)
            return element.text.strip()
        except NoSuchElementException:
            return ""

    def _extract_business_name(self) -> str:
        """
        Extract business name with multiple fallback selectors.

        Returns:
            Business name or empty string
        """
        # Try multiple selectors in order of preference
        selectors = [
            "h1.DUwDvf",                           # Primary selector
            "h1.fontHeadlineLarge",                # Alternative
            "div[role='main'] h1",                  # Generic h1 in main content
            "button[jsaction*='pane.placeInfo']",  # Place info button
        ]

        for selector in selectors:
            name = self._safe_extract_text(selector)
            if name:
                return name

        # Last resort: try to extract from title
        try:
            title = self.driver.title
            # Google Maps title format: "Business Name - Google Maps"
            if " - Google Maps" in title:
                name = title.split(" - Google Maps")[0].strip()
                if name and name != "Google Maps":
                    return name
        except:
            pass

        return ""

    def _extract_category(self) -> str:
        """Extract vendor category/type."""
        try:
            category_button = self.driver.find_element(By.CSS_SELECTOR, "button[jsaction*='category']")
            return category_button.text.strip()
        except:
            return ""

    def _extract_rating(self) -> str:
        """Extract rating score."""
        try:
            rating_element = self.driver.find_element(By.CSS_SELECTOR, "div.F7nice span[aria-hidden='true']")
            return rating_element.text.strip()
        except:
            return ""

    def _extract_reviews_count(self) -> str:
        """Extract number of reviews."""
        try:
            reviews_element = self.driver.find_element(By.CSS_SELECTOR, "div.F7nice span[aria-label*='reviews']")
            aria_label = reviews_element.get_attribute('aria-label')
            # Extract number from aria-label
            match = re.search(r'([\d,]+)\s*reviews?', aria_label)
            if match:
                return match.group(1)
            return ""
        except:
            return ""

    def _extract_phone(self) -> str:
        """Extract phone number."""
        try:
            # Click on phone button to reveal number
            phone_button = self.driver.find_element(By.CSS_SELECTOR, "button[data-item-id*='phone']")
            phone_text = phone_button.get_attribute('aria-label')

            # Extract phone from aria-label
            if phone_text:
                # Remove "Phone: " prefix if present
                phone = phone_text.replace('Phone:', '').strip()
                return phone

            return phone_button.text.strip()
        except:
            return ""

    def _extract_website(self) -> str:
        """Extract website URL."""
        try:
            website_link = self.driver.find_element(By.CSS_SELECTOR, "a[data-item-id='authority']")
            return website_link.get_attribute('href') or ""
        except:
            return ""

    def close(self):
        """Close the browser."""
        if self.driver:
            self.driver.quit()
            print("\n✅ Browser closed")
