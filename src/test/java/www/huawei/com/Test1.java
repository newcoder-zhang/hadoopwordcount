package www.huawei.com;

public class Test1 {

    public static void main(String[] args) {
        String str="121508281810000000\thttp://www.yhd.com/?union_ref=7&cp=0\t\t\t3\tPR4E9HWE38DMN4Z6HUG667SCJNZXMHSPJRER\t\t\t\t\tVFA5QRQ1N4UJNS9P6MH6HPA76SXZ737P\t10977119545\t\t124.65.159.122\t\tunionKey:10977119545\t\t2015-08-28 18:10:00\t50116447\thttp://image.yihaodianimg.com/virtual-web_static/virtual_yhd_iframe_index_widthscreen.html?randid=2015828\t6\t\t\t\t1000\t\t\t\t\tMozilla/5.0 (Windows NT 6.1; rv:40.0) Gecko/20100101 Firefox/40.0\tWin32\t\t\t\t\tlunbo_tab_3\t\t北京市\t2\t\t\t北京市\t\t\t\t\t\t1\t\t1\t1\t\t1\t\t\t\t\t\t\t\t\t\t\t1440*900\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t1440756285639\n";
        String[] strs = str.split("\t");
        for (int i=0;i<strs.length;i++) {
            System.out.println(i+"..."+strs[i]);
        }
    }
}
