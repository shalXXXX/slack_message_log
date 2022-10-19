import requests
import pandas as pd
from tqdm import tqdm

def get_reponse(api_url, payload=None):
  BASE_URL = "https://slack.com/api/"
  token = 'XXXXXXXXXXXXXX'

  headers = {
    "Authorization": "Bearer {}".format(token)
  }

  url = BASE_URL + api_url

  if payload:
    res = requests.get(url, headers=headers, params=payload)
    return res.json()
  else:
    res = requests.get(url, headers=headers)
    return res.json()

def get_messages(channel_id, channel_name, users_dict):
  api_url = "conversations.history"
  payload = {
    "channel": channel_id,
    "limit": 1000
  }
  message_logs = get_reponse(api_url, payload=payload)

  message_df = pd.DataFrame(columns=["thread", "user", "message", "reply_count", "type"])
  reply_api_url = "conversations.replies"
  for item in tqdm(message_logs["messages"]):
    msg = item["text"]
    sender = item["user"]
    for user in users_dict.keys():
      if user in msg:
        msg = msg.replace(user, users_dict[user])
    sender = sender.replace(sender, users_dict[sender])


    if "reply_count" in item:
      payload = {
        "channel": channel_id,
        "ts": item["ts"]
      }
      replies = get_reponse(reply_api_url, payload=payload)
      reply_count = item["reply_count"]
      reply_df = pd.DataFrame(columns=["thread", "user", "message", "reply_count", "type"])
      for reply in replies["messages"]:
        rep = reply["text"]
        user_name = reply["user"]
        for user in users_dict.keys():
          if user in rep:
            rep = rep.replace(user, users_dict[user])
        user_name = user_name.replace(user_name, users_dict[user_name])
        reply_df = reply_df.append({"thread": reply["thread_ts"], "user": user_name, "message": rep,"reply_count": 0, "type": "reply"}, ignore_index=True)
      reply_df = reply_df.drop(reply_df.index[0])
      reply_df = reply_df.iloc[::-1]
      message_df = pd.concat([message_df, reply_df])
    else:
      reply_count = 0
    message_df = message_df.append({"thread": item["ts"], "user": sender, "message": msg,"reply_count": reply_count, "type": "message"}, ignore_index=True)

  message_df_sorted = message_df[::-1]
  new_df = message_df_sorted[["user", "message", "reply_count", "type", "thread"]]

  output_path = "output/" + channel_name + "_message_history.csv"
  new_df.to_csv(output_path, encoding="utf-8", index=False)

if __name__ == "__main__":
  user_url = "users.list"
  users = get_reponse(user_url)
  users_dict = {}
  for user in users["members"]:
    users_dict[user["id"]] = user["name"]

  channels_url = "conversations.list"
  channels = get_reponse(channels_url)

  channel_list = ["取得したいチャンネル"]

  for channel in channels["channels"]:
    if not channel["name"] in channel_list:
      continue
    print(channel["id"], channel["name"])
    get_messages(channel["id"], channel["name"], users_dict)