import requests

data = {
    "name": "Dilip Choudhary",
    "email": "dilipchoudhary9807@gmail.com",
    "phone": "+919309951072",
    "linkedin_url": "https://www.linkedin.com/in/dilip-choudhary-39966421a/"
}

response = requests.post(
    "https://pseudogram-api.onrender.com/v1/apply",
    json=data
)

print(response.status_code)
print(response.text)