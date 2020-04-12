package com.byterun.utils;

import com.byterun.model.ContentModel;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class HtmlParseUtil {

//    public static void main(String[] args) throws IOException {
//        List<ContentModel> list = parseHtml("java");
//        System.out.println(JSON.toJSONString(list));
//
//    }

    public static List<ContentModel> parseHtml(String keyword) throws IOException {
        // 获取请求
        String url = "https://search.jd.com/Search?keyword="+keyword;
        // 解析网页,jsoup返回document对象
        Document document = Jsoup.parse(new URL(url), 30000);
        // 所有js中使用的方法
        Element j_goodsList = document.getElementById("J_goodsList");
        //System.out.println(j_goodsList.html());
        // 获取所有的li元素
        Elements li = j_goodsList.getElementsByTag("li");
        List<ContentModel> list = new ArrayList<>();
        for (Element element : li) {
            // 图片延迟加载（无法获取），可以获取懒加载地址
            //String img = element.getElementsByTag("img").eq(0).attr("src");
            String img = element.getElementsByTag("img").eq(0).attr("source-data-lazy-img");
            String price = element.getElementsByClass("p-price").eq(0).text();
            String title = element.getElementsByClass("p-name").eq(0).text();
            ContentModel model = new ContentModel();
            model.setImg(img)
                    .setPrice(price)
                    .setTitle(title);
            list.add(model);
        }

        return list;
    }
}
