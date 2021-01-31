# Collecting URL

We need the url for each company website as the starting point for the scraper to crawl. 


# Scraper

The scraper needs to navigate through each company website, accessing every internal link. While recursively traversing websites, the scraper flags only the web pages that suggest sustainability content, i.e. at least one of the predefined keywords is found in the URL address of the page or the text of the hyperlink leading to the page. Keywords were chosen by inspecting a sample of websites and looking at what were the most common words used to introduce sustainability content. The list of keywords is as follows: environment, sustainab, responsib $^{1}$, footprint. Once a page is found, it needs to be cleaned before it is saved in a MongoDB $^{2}$ database. A high-level overview of the architecture of the scraper is shown in Figure 1

## breath-first search

## Content extraction algorithms

## Flow chart

```
├── scrapy.cfg
└── stack
    ├── __init__.py
    ├── items.py
    ├── pipelines.py
    ├── settings.py
    └── spiders
        └── __init__.py 
```

# Mongodb

start local instance: in terminal, go to mongodb repository first and enter `mongod`

# Reference

Sozzi, A., 2020. Onsbigdata/Measuring-Sustainability-Reporting. [GitHub](https://github.com/ONSBigData/Measuring-Sustainability-Reporting). 