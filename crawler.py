import pandas as pd
import requests
import time
import csv
import re
import html


def download_website(url, base_url):
    data = {"url": [], "html_data": []}
    response = requests.get(url)


    if response.status_code == 200:
        # data["url"].append(sub_url)
        get_links_pattern = r'<ul class="alpha">(.*?)</ul>'
        # data["html_data"].append(response.content.decode("utf-8"))
        matches = re.findall(get_links_pattern,response.content.decode("utf-8"), flags=re.S)
        get_separate_links_pattern = r'<li><a href="(.*?)"'
        links = re.findall(get_separate_links_pattern, matches[0], flags=re.S)
        
    for url_link in links:
        letter_url = base_url+str(url_link)
        second_response = requests.get(letter_url)
        pagination_pattern = r'<ul id="pagin">(.*?)</ul>'
        matches = re.findall(pagination_pattern, second_response.content.decode("utf-8"), flags = re.S)
        single_page_pattern = r'href="(https.*?)"'
        page_matches = re.findall(single_page_pattern, matches[0], flags = re.S)
        if not page_matches:
            page_matches = [letter_url]
        # print("ROUND DONE! -------------- {}".format(letter_url))


        for sub_page in page_matches:
            sub_page_response = requests.get(sub_page)
            get_name_links_pattern = r'<a href="(/baby-names/detail.*?)">'
            matches = re.findall(get_name_links_pattern, sub_page_response.content.decode("utf-8"), flags = re.S)

            for name_page in matches:
                full_name_page = base_url + str(name_page)
                name_page_response = requests.get(full_name_page)
                data["url"].append(full_name_page)
                data["html_data"].append(name_page_response.content)

        time.sleep(1)

    return data


def write_raw_data(url, base_url):
    data = download_website(url, base_url)

    df = pd.DataFrame({"link": data["url"], "data": data["html_data"]})

    df.to_csv('raw_data.csv', index=False)


def scrape_data(link, content, name_data):
    content = html.unescape(content)
    pattern_baby_name = r'<div class="cal-row"[\s\S]*?<div class="sm-col"><p>Baby Name:</p></div>[\s\S]*?<span>(.*?)</span>'
    pattern_gender = r'<div class="cal-row"[\s\S]*?<div class="sm-col"><p>Gender:</p></div>[\s\S]*?<span>(.*?)</span>'
    pattern_pronunciation = r'<div class="cal-row"[\s\S]*?<div class="sm-col"><p>Pronunciation:</p></div>[\s\S]*?<span>(.*?)</span>'
    pattern_origin = r'<div class="cal-row"[\s\S]*?<div class="sm-col"><p>Origin:</p></div>[\s\S]*?<span>(.*?)</span>'
    pattern_meaning = r'<div class="cal-row"[\s\S]*?<div class="sm-col"><p>Meaning:</p></div>[\s\S]*?<span>(.*?)</span>'
    pattern_facts = r'<div class="cal-row"[\s\S]*?<div class="sm-col"><p>Facts:</p></div>[\s\S]*?<span>(.*?)</span>'
    pattern_synonym = r'<div class="cal-row"[\s\S]*?<div class="sm-col"><p>Synonym:</p></div>[\s\S]*?<span>(.*?)</span>'
    match_baby_name = re.findall(pattern_baby_name, content, flags = re.S)
    match_gender = re.findall(pattern_gender, content, flags = re.S)
    match_pronunciation = re.findall(pattern_pronunciation, content, flags = re.S)
    match_origin = re.findall(pattern_origin, content, flags = re.S)
    match_meaning = re.findall(pattern_meaning, content, flags = re.S)
    match_facts = re.findall(pattern_facts, content, flags = re.S)
    match_synonym = re.findall(pattern_synonym, content, flags = re.S)
    if match_baby_name:
        name_data["Name"].append(match_baby_name[0])
        name_data["Gender"].append(match_gender[0])
        name_data["Pronunciation"].append(match_pronunciation[0])
        name_data["Origin"].append(match_origin[0])
        name_data["Meaning"].append(match_meaning[0])
        name_data["Facts"].append(match_facts[0])
        name_data["Synonym"].append(match_synonym[0])


def write_scraped_data():

    downloaded_data_df = pd.read_csv("raw_data.csv")

    name_data = {"Name": [],"Gender": [],"Pronunciation": [], "Origin": [], "Meaning": [], "Facts": [], "Synonym": []}

    for index in downloaded_data_df.index:
        scrape_data(downloaded_data_df["link"][index], downloaded_data_df["data"][index], name_data)
        # print(downloaded_data_df["link"][index])

    name_data_df = pd.DataFrame({"Name": name_data["Name"], "Gender": name_data["Gender"], "Pronunciation": name_data["Pronunciation"], "Origin": name_data["Origin"], "Meaning": name_data["Meaning"], "Facts": name_data["Facts"], "Synonym": name_data["Synonym"]})

    name_data_df.to_csv("name_data.csv", index=False)





# write_raw_data('https://www.emmasdiary.co.uk/baby-names/baby-names-beginning-with-a', "https://www.emmasdiary.co.uk")
# write_scraped_data()