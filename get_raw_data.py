import os
import time
import csv
import shutil
import threading
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException


class SoilHealthScraper:
    def __init__(self, nutrient_type, chrome_port=None, skip_years=None):
        self.nutrient_type = nutrient_type
        self.chrome_port = chrome_port
        # Set years to skip (can be list of years or single year)
        self.skip_years = skip_years if skip_years else []
        if isinstance(self.skip_years, str):
            self.skip_years = [self.skip_years]

        # Setup Chrome with different ports to avoid conflicts
        chrome_options = webdriver.ChromeOptions()
        if chrome_port:
            chrome_options.add_argument(f"--remote-debugging-port={chrome_port}")

        service = Service(r'C:\chromedriver-win64\chromedriver-win64\chromedriver.exe')
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.get("https://soilhealth.dac.gov.in/piechart")
        self.wait = WebDriverWait(self.driver, 20)

        # XPaths
        self.year_xpath = "(//div[@role='combobox' and contains(@class, 'MuiSelect-select')])[3]"
        self.state_xpath = "(//div[@role='combobox' and contains(@class, 'MuiSelect-select')])[4]"
        self.district_xpath = "(//div[@role='combobox' and contains(@class, 'MuiSelect-select')])[5]"
        self.block_xpath = "(//div[@role='combobox' and contains(@class, 'MuiSelect-select')])[6]"

    def should_skip_year(self, year_name):
        """Check if year should be skipped"""
        # Check if year is in skip list
        if year_name in self.skip_years:
            return True

        # Check if data directory already exists for this year
        year_data_path = f"data/raw/{year_name}"
        if os.path.exists(year_data_path):
            # Check if directory has substantial data (not empty)
            if self.has_existing_data(year_data_path):
                print(f"📁 [{self.nutrient_type}] Found existing data for {year_name}, skipping...")
                return True

        return False

    def has_existing_data(self, year_path):
        """Check if year directory has existing CSV files"""
        try:
            csv_count = 0
            for root, dirs, files in os.walk(year_path):
                csv_count += len([f for f in files if f.endswith('.csv') and self.nutrient_type.lower() in f.lower()])

            # Consider it has data if there are at least 5 CSV files for this nutrient type
            return csv_count >= 5
        except Exception:
            return False

    def safe_click(self, el):
        try:
            el.click()
        except:
            self.driver.execute_script("arguments[0].click();", el)

    def reset_page(self):
        """Reset the page to initial state"""
        try:
            self.driver.refresh()
            time.sleep(3)
            # Re-select nutrient type
            button_text = f"{self.nutrient_type}(Table View)"
            button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, f"//button[contains(text(), '{button_text}')]")))
            button.click()
            time.sleep(2)
            return True
        except Exception as e:
            print(f"⚠️  [{self.nutrient_type}] Page reset failed: {e}")
            return False

    def get_dropdown_options(self, xpath, retries=3):
        """Get dropdown options with retry logic - returns option texts instead of elements"""
        for attempt in range(retries):
            try:
                dropdown = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
                self.safe_click(dropdown)
                time.sleep(1)
                options = self.wait.until(
                    EC.presence_of_all_elements_located(
                        (By.XPATH, "//ul[@role='listbox']/li[not(@aria-disabled='true')]")))

                # Extract text content immediately to avoid stale references
                option_texts = []
                for option in options:
                    try:
                        text = option.text.strip()
                        if text:  # Only add non-empty options
                            option_texts.append(text)
                    except StaleElementReferenceException:
                        continue

                # Close dropdown by clicking elsewhere
                try:
                    self.driver.find_element(By.TAG_NAME, "body").click()
                    time.sleep(0.5)
                except:
                    pass

                return option_texts
            except (TimeoutException, StaleElementReferenceException) as e:
                print(f"⚠️  [{self.nutrient_type}] Attempt {attempt + 1} failed for dropdown: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
                    # Try refreshing page on final retry
                    if attempt == retries - 2:
                        self.reset_page()
                else:
                    return []
        return []

    def select_dropdown_by_text(self, xpath, target_text, max_retries=3):
        """Select dropdown option by text with retry logic"""
        for attempt in range(max_retries):
            try:
                dropdown = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
                self.safe_click(dropdown)
                time.sleep(1)

                # Find and click the option with matching text
                option_xpath = f"//ul[@role='listbox']/li[normalize-space(text())='{target_text}']"
                option = self.wait.until(EC.element_to_be_clickable((By.XPATH, option_xpath)))
                self.safe_click(option)
                time.sleep(1)
                return True
            except (StaleElementReferenceException, TimeoutException, NoSuchElementException) as e:
                print(
                    f"⚠️ [{self.nutrient_type}] Dropdown selection attempt {attempt + 1} failed for '{target_text}': {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    # Try closing any open dropdowns
                    try:
                        self.driver.find_element(By.TAG_NAME, "body").click()
                        time.sleep(0.5)
                    except:
                        pass
        return False

    def scrape_table(self):
        time.sleep(2)
        try:
            scroller = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "MuiDataGrid-virtualScroller")))
            rows = scroller.find_elements(By.CLASS_NAME, "MuiDataGrid-row")
            data = []
            for row in rows:
                cells = row.find_elements(By.CLASS_NAME, "MuiDataGrid-cell")
                row_data = [cell.text.strip() for cell in cells]
                if any(cell != "0" and cell != "" for cell in row_data):
                    data.append(row_data)
            return data
        except Exception as e:
            print(f"⚠️ [{self.nutrient_type}] Table scraping failed: {e}")
            return []

    def download_and_rename_csv(self, year, state, district, block):
        try:
            download_link = self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(text(),'Export to CSV')]")))
            self.driver.execute_script("arguments[0].click();", download_link)
            time.sleep(3)  # Increased wait time for download

            download_folder = os.path.expanduser("~/Downloads")
            downloaded_file = os.path.join(download_folder, "my-file.csv")

            # Wait longer for download to complete
            max_wait = 10
            wait_count = 0
            while not os.path.exists(downloaded_file) and wait_count < max_wait:
                time.sleep(1)
                wait_count += 1

            if os.path.exists(downloaded_file):
                directory = f"data/raw/{year}/{state}/{district}"
                os.makedirs(directory, exist_ok=True)
                new_file = os.path.join(directory, f"{block}_{self.nutrient_type.lower()}.csv")
                shutil.move(downloaded_file, new_file)
                print(f"✅ [{self.nutrient_type}] Downloaded and saved: {new_file}")
            else:
                print(f"⚠️ [{self.nutrient_type}] Downloaded file not found")
        except Exception as e:
            print(f"⚠️ [{self.nutrient_type}] CSV Download failed: {e}")

    def start_scraping(self):
        """Main scraping logic with improved stale element handling"""
        try:
            # Select nutrient type
            button_text = f"{self.nutrient_type}(Table View)"
            button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, f"//button[contains(text(), '{button_text}')]")))
            button.click()
            time.sleep(2)
            print(f"🚀 [{self.nutrient_type}] Started scraping")
        except Exception as e:
            print(f"⚠️ [{self.nutrient_type}] Failed to select {button_text}: {e}")
            self.driver.quit()
            return

        try:
            # Get all year options as text
            year_options = self.get_dropdown_options(self.year_xpath)
            if not year_options:
                print(f"❌ [{self.nutrient_type}] No year options found")
                self.driver.quit()
                return

            print(f"📅 [{self.nutrient_type}] Found {len(year_options)} years: {year_options}")

            for year_index, year_name in enumerate(year_options):
                # Check if year should be skipped
                if self.should_skip_year(year_name):
                    print(f"⏭️ [{self.nutrient_type}] Skipping year: {year_name} (already processed or in skip list)")
                    continue

                # Always reselect year from fresh dropdown to ensure we're on the right year
                print(f"\n📅 [{self.nutrient_type}] Selecting Year: {year_name} ({year_index + 1}/{len(year_options)})")

                # Get fresh year options and select by text
                current_year_options = self.get_dropdown_options(self.year_xpath)
                if not current_year_options or year_name not in current_year_options:
                    print(f"⚠️ [{self.nutrient_type}] Year {year_name} not available in fresh dropdown")
                    continue

                if not self.select_dropdown_by_text(self.year_xpath, year_name):
                    print(f"⚠️ [{self.nutrient_type}] Failed to select year: {year_name}")
                    continue

                print(f"✅ [{self.nutrient_type}] Successfully selected Year: {year_name}")

                # Get fresh state options for this year
                state_options = self.get_dropdown_options(self.state_xpath)
                if not state_options:
                    print(f"⏭️ [{self.nutrient_type}] No states found for year {year_name}")
                    continue

                print(f"🏛️ [{self.nutrient_type}] Found {len(state_options)} states for {year_name}")

                for state_index, state_name in enumerate(state_options):
                    # Clean state name for file system
                    clean_state_name = state_name.replace("/", "-").replace(" ", "_")

                    # Select state by text
                    if not self.select_dropdown_by_text(self.state_xpath, state_name):
                        print(f"⚠️ [{self.nutrient_type}] Failed to select state: {state_name}")
                        continue

                    print(f"\n🏛️ [{self.nutrient_type}] Processing state: {state_name}")

                    # Check if entire state is empty
                    state_data = self.scrape_table()
                    if not state_data:
                        print(f"⏭️ [{self.nutrient_type}] Skipping state: {state_name} — empty table")
                        continue

                    # Get district options
                    district_options = self.get_dropdown_options(self.district_xpath)
                    if not district_options:
                        print(f"⏭️ [{self.nutrient_type}] No districts found for state {state_name}")
                        continue

                    print(f"📍 [{self.nutrient_type}] Found {len(district_options)} districts for {state_name}")
                    valid_district_found = False

                    for district_index, district_name in enumerate(district_options):
                        try:
                            # Clean district name for file system
                            clean_district_name = district_name.replace("/", "-").replace(" ", "_")

                            print(
                                f"  📍 [{self.nutrient_type}] Processing district: {district_name} ({district_index + 1}/{len(district_options)} in {state_name})")

                            # Select district by text (year and state are already selected)
                            if not self.select_dropdown_by_text(self.district_xpath, district_name):
                                print(f"    ⚠️ [{self.nutrient_type}] Failed to select district: {district_name}")
                                continue

                            # Check if district has any data first
                            district_data = self.scrape_table()
                            if not district_data:
                                print(f"  ⏭️ [{self.nutrient_type}] Skipping district {district_name} — no data found")
                                continue

                            # District has data, mark as valid
                            valid_district_found = True

                            # Check if this is specifically "All Districts" option
                            if district_name in ["All_Districts", "All Districts"]:
                                print(f"  📊 [{self.nutrient_type}] Found 'All Districts' option: {district_name}")
                                print(f"  ⬇️ [{self.nutrient_type}] Downloading aggregated CSV and skipping blocks")
                                self.download_and_rename_csv(year_name, clean_state_name, clean_district_name,
                                                             "AllDistricts")
                                continue  # Skip block processing and move to next district

                            # Try to get block options for regular districts
                            block_options = self.get_dropdown_options(self.block_xpath)

                            if not block_options:
                                print(
                                    f"  ⏭️ [{self.nutrient_type}] No blocks for {district_name} — saving district-level CSV")
                                self.download_and_rename_csv(year_name, clean_state_name, clean_district_name,
                                                             "NoBlock")
                                continue

                            print(f"  🏘️ [{self.nutrient_type}] Found {len(block_options)} blocks for {district_name}")

                            # Process each block (only for regular districts)
                            for block_index, block_name in enumerate(block_options):
                                try:
                                    # Clean block name for file system
                                    clean_block_name = block_name.replace("/", "-").replace(" ", "_")

                                    print(
                                        f"    🏘️ [{self.nutrient_type}] Processing block: {block_name} ({block_index + 1}/{len(block_options)} in {district_name})")

                                    # Select block by text (year, state, and district are already selected)
                                    if not self.select_dropdown_by_text(self.block_xpath, block_name):
                                        print(f"      ⚠️ [{self.nutrient_type}] Failed to select block: {block_name}")
                                        continue

                                    data = self.scrape_table()
                                    if data:
                                        self.download_and_rename_csv(year_name, clean_state_name, clean_district_name,
                                                                     clean_block_name)
                                    else:
                                        print(f"    ⏭️ [{self.nutrient_type}] No data found for block: {block_name}")

                                except Exception as e:
                                    print(f"    ⚠️ [{self.nutrient_type}] Block processing error for {block_name}: {e}")
                                    continue

                        except Exception as e:
                            print(f"  ⚠️ [{self.nutrient_type}] District processing error for {district_name}: {e}")
                            continue

                    if not valid_district_found:
                        print(f"⏭️ [{self.nutrient_type}] No valid data found for state: {state_name}")

                    print(f"✅ [{self.nutrient_type}] Finished state: {state_name}")

                print(f"✅ [{self.nutrient_type}] Finished year: {year_name}")

        except Exception as e:
            print(f"❌ [{self.nutrient_type}] Critical error in main loop: {e}")
            import traceback
            traceback.print_exc()
        finally:
            print(f"\n🏁 [{self.nutrient_type}] Scraping completed. Closing browser...")
            self.driver.quit()


def run_scraper(nutrient_type, chrome_port, skip_years=None):
    """Function to run scraper in a thread"""
    scraper = SoilHealthScraper(nutrient_type, chrome_port, skip_years)
    scraper.start_scraping()


if __name__ == "__main__":
    print("🚀 Starting dual nutrient scraping...")

    # Define years to skip (modify as needed)
    years_to_skip = ["2025-26"]  # Add more years as needed: ["2025-26", "2024-25"]

    # Create threads for both scrapers with skip_years parameter
    macro_thread = threading.Thread(target=run_scraper, args=("MacroNutrient", 9222, years_to_skip))
    micro_thread = threading.Thread(target=run_scraper, args=("MicroNutrient", 9223, years_to_skip))

    # Start both threads
    macro_thread.start()
    micro_thread.start()

    # Wait for both threads to complete
    macro_thread.join()
    micro_thread.join()

    print("🎉 Both scrapers completed!")