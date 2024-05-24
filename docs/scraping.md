# rate limit bypass

_ddos protection_

-   rotating geolocation: no effect
-   rotating IP with VPN: works for most but some IPs, but some are blocked because of user abuse

_rate limit_

-   buffered page load
    -   set the pagination to 5 entries per page only, or else you'll have to deal with scrolling and buffered loading of entries.
    -   alternatively config to 100 entries per page so you don't get rate limited as easily.

# frameworks

-   https://github.com/microsoft/playwright-python
    -   https://github.com/AtuboDad/playwright_stealth
    -   https://github.com/Granitosaurus/playwright-stealth
-   https://github.com/SeleniumHQ/selenium ❌
    -   https://github.com/ultrafunkamsterdam/undetected-chromedriver
    -   https://github.com/ultrafunkamsterdam/nodriver
