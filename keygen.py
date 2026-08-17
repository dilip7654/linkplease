import requests

response = requests.post(
    "https://pseudogram-api.onrender.com/v1/keygen",
    json={
        "email": "dilipchoudhary9807@gmail.com"
    }
)

print(response.status_code)
print(response.text)