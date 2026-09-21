import threading
import time
import webbrowser
import uvicorn

URL = "http://localhost:8020"


def _open_browser():
    time.sleep(1.5)
    webbrowser.open(URL)


if __name__ == "__main__":
    threading.Thread(target=_open_browser, daemon=True).start()
    # reload=False: reload กลางคันจะทำให้ background scan job ที่รันอยู่หลุดการติดตาม
    uvicorn.run("main:app", host="0.0.0.0", port=8020, reload=False)
