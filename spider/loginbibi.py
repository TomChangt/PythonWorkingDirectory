import time
import os
from random import uniform
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from chaojiying import Chaojiying_Client
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


def login(url: str, username: str, password: str) -> None:
    cjy = (
        os.getenv("CJY_U_NAME"),
        os.getenv("CJY_PWD"),
        os.getenv("CJY_SOFT_ID"),
    )
    chrome_options = Options()
    chrome_options.debugger_address = "127.0.0.1:9222"

    service = Service(executable_path="/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                '//*[@id="i_cecream"]/div[2]/div[1]/div[1]/ul[2]/li[1]/li/div[1]/div',
            )
        )
    ).click()

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, "/html/body/div[4]/div/div[4]/div[2]/form/div[1]/input")
        )
    ).send_keys(username)
    time.sleep(uniform(0.5, 1.5))

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, "/html/body/div[4]/div/div[4]/div[2]/form/div[3]/input")
        )
    ).send_keys(password)
    time.sleep(uniform(0.5, 1.5))

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                "/html/body/div[4]/div/div[4]/div[2]/div[2]/div[2]",
            )
        )
    ).click()

    pic = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, '//div[@class="geetest_widget"]'))
    )
    # 解决截屏不完全
    time.sleep(uniform(1, 1.5))
    img = pic.screenshot_as_png
    width, height = pic.size["width"], pic.size["height"]

    chaojiying = Chaojiying_Client(*cjy)
    locs = chaojiying.PostPic(img, 9004)["pic_str"]
    locs = locs.split("|")
    for loc in locs:
        x, y = loc.split(",")
        x = int(x) - int(width / 2)
        y = int(y) - int(height / 2)

        print(f"点击....{x,y}")
        ActionChains(driver).move_to_element_with_offset(pic, x, y).click().perform()
        time.sleep(uniform(0.5, 1.5))

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                '//div[@class="geetest_commit_tip"]',
            )
        )
    ).click()


if __name__ == "__main__":
    login("https://www.bilibili.com", "18368466666", "123456")
