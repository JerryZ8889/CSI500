#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Open the Streamlit dashboard in a real (headless) browser so it does not sleep.

Streamlit Community Cloud only counts a visit when a browser actually loads the
app, and a sleeping app only wakes when someone clicks its wake button. A plain
HTTP GET (the old cron-job.org keepalive) does neither, so this script:
- opens the page in Chromium
- clicks "Yes, get this app back up!" if the app is asleep
- waits until the app itself has loaded, otherwise exits non-zero
"""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

URL = "https://csi500.streamlit.app/"
WAKE_BUTTON = "Yes, get this app back up!"
LOAD_TIMEOUT_S = 360


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 1000})
        page.goto(URL, wait_until="networkidle", timeout=90_000)

        body = page.locator("body").inner_text()
        if "source IP address not allowed" in body:
            print("Blocked: Streamlit refused this runner's IP address.")
            return 1

        button = page.get_by_role("button", name=WAKE_BUTTON)
        if button.count():
            print("App was asleep, clicking the wake button.")
            button.click()
        else:
            print("App was already awake.")

        app = page.frame_locator("iframe[title='streamlitApp']").locator("[data-testid='stApp']")
        deadline = time.time() + LOAD_TIMEOUT_S
        while time.time() < deadline:
            if app.count():
                print("App loaded.")
                browser.close()
                return 0
            time.sleep(10)

        page.screenshot(path="wake_failed.png")
        print(f"App did not load within {LOAD_TIMEOUT_S}s.")
        browser.close()
        return 1


if __name__ == "__main__":
    sys.exit(main())
