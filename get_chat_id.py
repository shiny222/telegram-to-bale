import json

with open("config.json", encoding="utf-8") as f:
    config = json.load(f)

TOKEN = config["BALE_BOT_TOKEN"]
print(f"https://tapi.bale.ai/bot{TOKEN}/getUpdates")
