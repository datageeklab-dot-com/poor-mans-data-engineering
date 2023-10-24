import pandas as pd

def extract_data(api_response):
    """
    Extract relevant data from the API response.

    Args:
        api_response (dict): JSON data from the API response.

    Returns:
        pandas.DataFrame: Extracted data in a DataFrame.
    """
    feed = api_response.get("feed", [])
    news_data = []
    
    for item in feed:
        title = item.get("title", "")
        time_published = item.get("time_published", "")
        authors = ", ".join(item.get("authors", []))
        summary = item.get("summary", "")
        source = item.get("source", "")
        source_domain = item.get("source_domain", "")
    
        news_data.append({
            "title": title,
            "time_published": time_published,
            "authors": authors,
            "summary": summary,
            "source": source,
            "source_domain": source_domain
        })

    return pd.DataFrame(news_data)
