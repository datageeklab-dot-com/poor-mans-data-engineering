import requests



def fetch_data(api_url, api_key,time_from):
    """
    Fetch data from the API using the provided URL and API key.

    Args:
        api_url (str): The URL of the API.
        api_key (str): Your API key.
        time_from (str): Last run timestamp

    Returns:
        dict: JSON data from the API response.
    """
    final_api_url=f"{api_url}&time_from={time_from}&apikey={api_key}"
    response = requests.get(final_api_url)
    return response.json()
