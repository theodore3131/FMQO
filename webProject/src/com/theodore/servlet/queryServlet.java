package com.theodore.servlet;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import com.google.gson.Gson;
import multiQueryOptJoin.src.Common.FullQuery;
import multiQueryOptJoin.src.mqo_gq.MultiQuery_gq_Hybrid;

@WebServlet(name = "queryServlet")
public class queryServlet extends HttpServlet {
    private static String[] queries;
    private static ArrayList<FullQuery> allQueryList;
    public queryServlet(){};
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/text; charset=utf-8");
//        Gson gson = new Gson();
        String SPARQLquery = request.getParameter("query");
        queries = SPARQLquery.split("\n");
        allQueryList = MultiQuery_gq_Hybrid.query(SPARQLquery);
        int windowSize = 2;
        int[] queryNumArr ={2};

//        String jsonObj1 = gson.toJson(queries);
//        String jsonObj2 = gson.toJson(allQueryList);

        PrintWriter out = response.getWriter();
        StringBuilder sb = new StringBuilder();
        for (int queryIdx = 0; queryIdx < queryNumArr[0]; queryIdx++) {
            FullQuery curFullQuery = allQueryList.get(queryIdx);
            sb.append("==== there are "+ curFullQuery.getResultList().size()+ " results for query " + (queryIdx+1) + " ====" + "\n");
        }

        out.write(sb.toString());
    }

    public static String[] getQueries() {
        if (queries.length==0)
            return null;
        return queries;
    }

    public static ArrayList<FullQuery> getAllQueryList() {
        if (allQueryList.size()==0)
            return null;
        return allQueryList;
    }
}
