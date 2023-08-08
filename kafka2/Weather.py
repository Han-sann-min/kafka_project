# https://openweathermap.org/ 에서 실시간 서울 날씨 받아오기

import requests
import json

city = "Seoul"
apikey = '31563b64da6452ee368abb8f9e78d807'
lang = "kr"

# 요청하기 위한 서버 주소, units=metric (섭씨온도로 변경)
api = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={apikey}&lang={lang}&units=metric" # 요청하기 위한 서버 주소

# 받아온 데이터는 문자열 형태
result = requests.get(api)
type(result.text) 

# Json 형태로 받아오기
data = json.loads(result.text)
type(data)

print(data["name"],"의 날씨입니다.")
print("날씨는 ", data["weather"][0]["description"],"입니다.")
print("현재 온도는 ", data["main"]["temp"],"입니다.")
print("하지만 체감 온도는 ", data["main"]["feels_like"],"입니다.")
print("최저 기온은 ",data["main"]["temp_min"],"입니다.") 
print("최고 기온은 ",data["main"]["temp_max"],"입니다.") 
print("습도는 ",data["main"]["humidity"],"입니다.")
print("기압은 ",data["main"]["pressure"],"입니다.")
print("풍향은 ",data["wind"]["deg"],"입니다.")
print("풍속은 ",data["wind"]["speed"],"입니다.")