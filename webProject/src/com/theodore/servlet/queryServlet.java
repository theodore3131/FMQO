package com.theodore.servlet;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.*;

import multiQueryOptJoin.src.Common.FullQuery;
import multiQueryOptJoin.src.mqo_gq.MultiQuery_gq_Hybrid;

@WebServlet(name = "queryServlet")
public class queryServlet extends HttpServlet {

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/text; charset=utf-8");

        String SPARQLquery = "";
        StringBuilder sb = new StringBuilder();
        String name, value = "";
        int queryNum = 0;
        for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
            name = entry.getKey();
            if (name.contains("query")){
                value = entry.getValue()[0];
                sb.append(value).append('\n');
                queryNum++;
            }
        }
        SPARQLquery = sb.toString();

        String[] queries = SPARQLquery.split("\n");

        String configs[] = request.getParameterValues("config");

        String configString = "";
        for (String item : configs) {
            configString += item;
        }

        int config;
        String pattern = "(0|1)*23*";

        boolean isMatch = Pattern.matches(pattern, configString);
        if (isMatch) {
            if (configString.contains("23"))
                config = 7;
            else
                config = 2;
        }
        else {
            switch (configString) {
                case "01":
                    config = 4;
                    break;
                case "03":
                    config = 5;
                    break;
                case "13":
                    config = 6;
                    break;
                case "013":
                    config = 8;
                    break;
                default:
                    config = Integer.parseInt(configString);
            }
        }

        MultiQuery_gq_Hybrid multiQuery_gq_hybrid = new MultiQuery_gq_Hybrid();
        Map<String, Object> map = multiQuery_gq_hybrid.query(SPARQLquery,config,queryNum);

        ArrayList<FullQuery> allQueryList = (ArrayList<FullQuery>)map.get("arrayList");
        String log = (String)map.get("log");
        String summary = (String)map.get("summary");

        int windowSize = 2;
        int[] queryNumArr ={2};

//        response.sendRedirect("result.jsp");

//        StringBuilder sb1 = new StringBuilder();
//        for (int queryIdx = 0; queryIdx < queryNumArr[0]; queryIdx++) {
//            FullQuery curFullQuery = allQueryList.get(queryIdx);
//            sb1.append("==== there are ").append(curFullQuery.getResultList().size()).append(" results for query ").append(queryIdx+1).append(" ====" + "\n");
//        }
//        request.setAttribute("result", sb1.toString());
        request.setAttribute("queries", queries);
        request.setAttribute("allQueryList", allQueryList);
        request.setAttribute("log", log);
        request.setAttribute("summary", summary);
        request.getRequestDispatcher("result.jsp").forward(request,response);
    }
}
