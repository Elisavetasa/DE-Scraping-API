from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv


def get_artists():
    driver = webdriver.Chrome()
    driver.get("https://kworb.net/spotify/listeners.html")
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    table = driver.find_element(By.TAG_NAME, "table")
    table_body = table.find_element(By.TAG_NAME, "tbody")
    artists = []

    while True:
        cookie_accept_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[2]/div[2]/div[2]/div[2]/button[1]")))
        cookie_accept_button.click()
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        wait.until(lambda driver: driver.execute_script("return document.body.scrollHeight;") > driver.execute_script(
            "return window.scrollY + window.innerHeight;"))
        driver.execute_script("window.scrollTo(0, 0);")
        break

    for row in table_body.find_elements(By.TAG_NAME, "tr"):
        columns = row.find_elements(By.TAG_NAME, "td")
        rank = int(columns[0].text.strip())
        name = columns[1].find_element(By.TAG_NAME, "a").text.strip()
        listeners = int(columns[2].text.strip().replace(",", ""))
        daily_change = int(columns[3].text.strip().replace(",", "").replace("+", "").replace("-", ""))
        peak_rank = int(columns[4].text.strip())
        peak_listeners = int(columns[5].text.strip().replace(",", ""))
        artist = {"rank": rank,
            "name": name,
            "listeners": listeners,
            "daily_change": daily_change,
            "peak_rank": peak_rank,
            "peak_listeners": peak_listeners}
        artists.append(artist)
    with open("artists.csv", "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["rank", "name", "listeners", "daily_change", "peak_rank", "peak_listeners"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(artists)

    driver.quit()
    print(f"Сохранено {len(artists)} артистов в artists.csv")


if __name__ == "__main__":
    get_artists()