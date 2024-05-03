import scrapy
from scrapy_playwright.page import PageMethod
from scrapy.http import HtmlResponse

class PwspideySpider(scrapy.Spider):
    name = "pwspidey"

    def start_requests(self):
        yield scrapy.Request(
            'https://www.cfainstitute.org/en/membership/professional-development/refresher-readings#sort=%40refreadingcurriculumyear%20descending',
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_methods=[
                    PageMethod('click', 'text="100"',timeout=10000),
                    PageMethod('evaluate', 'window.scrollTo(0, 0)'),
                    PageMethod('wait_for_selector', 'text="Results 1-100 of 224"'),
                ],
                page_number=1,
            ),
        )

    
    async def parse(self, response):
        page = response.meta["playwright_page"]
        page_number = 1

            # Process current page
        links = response.css('div.coveo-list-layout.CoveoResult a.CoveoResultLink::attr(href)').getall()
        print(f"Found {len(links)} links on page {page_number}")
        for link in links:
            yield scrapy.Request(
                response.urljoin(link),
                callback=self.parse_details,
                meta=dict(
                    playwright=True,
                    playwright_include_page=True,
                ),
            )

        while True:
            next_page_button = await page.query_selector('.coveo-pager-next-icon-svg')
            print("Checking for next page button ",next_page_button )  # Debugger 1
            if next_page_button:
                
                print("Next page button found. Clicking...")  # Debugger 2
                page_number += 1
                await next_page_button.click()

                if page_number == 2:
                    await page.wait_for_selector('text="Results 101-200 of 224"', timeout=60000)
                    print(f"Moved to page {page_number}.")  # Debugger 3
                    
                elif page_number == 3:
                    #await page.wait_for_selector('xpath=//span[contains(., "of 224")]', timeout=60000)
                    await page.wait_for_selector('text="Results 201-224 of 224"', timeout=60000)
                    print(f"Moved to page {page_number}.")  # Debugger 3
                    
                    
                new_page_html = await page.content()
                # Ensure the new response is created with the correct URL
                response = HtmlResponse(url=page.url, body=new_page_html.encode('utf-8'), encoding='utf-8')
                print("New response URL:", response.url)
                links = response.css('div.coveo-list-layout.CoveoResult a.CoveoResultLink::attr(href)').getall()
                print(f"Found {len(links)} links on the new page.")  # Debugger 4
                for link in links:
                    yield scrapy.Request(
                        response.urljoin(link),
                        callback=self.parse_details,
                        meta=dict(
                            playwright=True,
                            playwright_include_page=True,
                        ),
                    )
            
            else:
                    print("No next page button found. Ending pagination 2.")
                    await page.close()
                    break


    async def parse_details(self, response):
        introduction_texts = response.xpath(
            """
            //h2[contains(@class, 'article-section') and 
                (
                    contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'introduction') or
                    contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'overview')
                )
            ]/following-sibling::*[
                (self::p or self::ul or self::ol or self::br) and
                (preceding-sibling::h2[1][
                    contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'introduction') or
                    contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'overview')
                ])
            ]//text() | 
            //h2[contains(@class, 'article-section') and 
                (
                    contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'introduction') or
                    contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'overview')
                )
            ]/following-sibling::*[
                (self::p or self::ul or self::ol or self::br) and
                (preceding-sibling::h2[1][
                    contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'introduction') or
                    contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'overview')
                ])
            ]/li//text()
            """
        ).getall()

        learning_outcomes_list = response.xpath(
            "//h2[contains(@class, 'article-section') and contains(text(), 'Learning Outcomes')]/following-sibling::section[1]//ol[1]/li//text() | " +
            "//h2[contains(@class, 'article-section') and contains(text(), 'Learning Outcomes')]/following-sibling::section[1]//ul[1]/li//text()"
        ).getall()

        link_pdf = response.xpath("//a[contains(@class, 'locked-content') and not(contains(@class, 'underlined-anchor'))]//@href").get()
        base_domain = 'https://www.cfainstitute.org'

        # Use a conditional expression to handle None values for link_pdf
        pdf_link = base_domain + link_pdf if link_pdf else None

        combined_lo_text = ' '.join([item.strip() for item in learning_outcomes_list if item.strip()])
        combined_introduction_text = ' '.join([text.strip() for text in introduction_texts if text.strip()])

        yield {
            'Name of the topic': response.css('h1::text').get(),
            'Year': response.css('span.content-utility-curriculum::text').get(),
            'Level': response.css('span.content-utility-topic::text').get(),
            'Introduction Summary': combined_introduction_text,
            'Learning Outcomes': combined_lo_text,
            'Link to the Summary Page': response.url,
            'Link to the PDF File': pdf_link
        }
