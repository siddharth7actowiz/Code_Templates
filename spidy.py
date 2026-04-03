import scrapy

class SpidySpider(scrapy.Spider):
    name = "spidy"
    
    start_urls = ["https://www.tacomascrew.com/all-categories"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_urls = set()

    def parse(self, response):
        # Extract data from the response and yield items
        for cat in response.xpath('//div[contains(@class,"category-card")]'):
            cat_name = cat.xpath('.//a[contains(@class, "product-title")]//p/text()').get()
            category = (cat_name or "").strip()

            cat_url = cat.xpath('.//a[contains(@class, "product-title")]/@href').get()

            if cat_url:
                url = f"https://www.tacomascrew.com/api/v1/catalogpages?path={cat_url}"
                print(f"cat_name: {category}, url: {url}")
                yield scrapy.Request(url=url, callback=self.parse_sub_cats, meta={"category": category, "current_path": cat_url,"subcat":None})

    def parse_sub_cats(self, response):
        category = response.meta.get("category")
        curr_path = response.meta.get("current_path")
        subcat = response.meta.get("subcat")
        data = response.json()

        sub_cates = data.get("category", {}).get("subCategories") or data.get("subCategories") or []

        if sub_cates:
            for sub_cat in sub_cates:
                sub_name = (sub_cat.get("shortDescription") or sub_cat.get("name") or "").strip()
                sub_path = sub_cat.get("path")
                if not sub_path:
                    continue

                next_category = category or sub_cat.get("name")
                next_sub = f"{subcat} > {sub_name}" if subcat else sub_name

                api_url = f"https://www.tacomascrew.com/api/v1/catalogpages?path={sub_path}"
                yield scrapy.Request(
                    url=api_url,
                    callback=self.parse_sub_cats,
                    meta={
                        "category": next_category,
                        "current_path": sub_path,
                        "subcat": next_sub,
                    },
                )
        else:
            product_api = (
                "https://www.tacomascrew.com/api/v1/products"
                f"?categoryPath={curr_path}&page=1&pageSize=96"
            )
            yield scrapy.Request(
                url=product_api,
                callback=self.parse_products,
                meta={"category": category, "current_path": curr_path, "subcat": subcat},
            )

    def parse_products(self, response):
        category = response.meta.get("category")
        curr_path = response.meta.get("current_path")
        subcat = response.meta.get("subcat")
        data = response.json()
        products = data.get("products", [])

        if not products:
            return

        for p in products:
            url = response.urljoin(p.get("productDetailUrl") or "")
            if url in self.seen_urls:
                continue
            self.seen_urls.add(url)
            yield {
                "category": category,
                "subcat": subcat,
                "name": (p.get("shortDescription") or p.get("name") or "").strip(),
                "url": url,
            }        