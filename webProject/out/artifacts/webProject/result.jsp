<%@ page import="multiQueryOptJoin.src.Common.FullQuery" %>
<%@ page import="java.util.ArrayList" %>
<%@ page import="com.theodore.servlet.queryServlet" %><%--
  Created by IntelliJ IDEA.
  User: zhiweixu
  Date: 2019/2/14
  Time: 17:53
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>

    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <meta name="description" content="">
    <meta name="author" content="">

    <link rel="shortcut icon" href="WEB-INF/favicon.ico" type="image/x-icon">
    <link rel="icon" href="WEB-INF/favicon.ico" type="image/x-icon">

    <title>Federated Multi Query Optimization</title>

    <!-- Bootstrap core CSS -->
    <link href="vendor/bootstrap/css/bootstrap.min.css" rel="stylesheet">

    <!-- Bootstrap core JavaScript -->
    <script src="vendor/jquery/jquery.min.js"></script>
    <script src="vendor/bootstrap/js/bootstrap.bundle.min.js"></script>

</head>

<body>
    <!-- Navigation -->
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark static-top">
        <div class="container">
        <a class="navbar-brand" href="#">FMQO</a>
        <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarResponsive" aria-controls="navbarResponsive" aria-expanded="false" aria-label="Toggle navigation">
            <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarResponsive">
            <ul class="navbar-nav ml-auto">
                <li class="nav-item active">
                    <a class="nav-link" href="index.jsp">Home
                        <span class="sr-only">(current)</span>
                    </a>
                </li>
                <li class="nav-item">
                    <a class="nav-link" href="about.html">About</a>
                </li>
                <li class="nav-item">
                    <a class="nav-link" href="contact.html">Contact</a>
                </li>
            </ul>
        </div>
    </div>
    </nav>

    <div class="container">
        <a href="javascript:history.go(-1)">Back</a>
    </div>

    <div class="container">
        <div class="col-lg-12">
            <h2 class="text-center">----- FMQO -----</h2>
        </div>
    </div>

    <div class="container-fluid">
        <div class="row">
            <div class="col-md-4 offset-sm-1">
                <div class="row">
                    <h4 class="text-center">-- SPARQL Queries --</h4>
                    <div class="overflow-auto">
                        <%
                            String[] queries = (String[])request.getAttribute("queries");
                            for (int i=0; i<queries.length;i++) {
                                out.print("query"+ (i+1) + ": <xmp>" + queries[i] + "</xmp>");
                            }
                            String summary = (String)request.getAttribute("summary");
                            out.print("<xmp>"+summary+"</xmp>");
                        %>
                    </div>
                </div>
                <div class="row">
                    <h4 class="text-center">-- Intermediate Process --</h4>
                    <div class="overflow-auto">
                        <%
                            String log = (String)request.getAttribute("log");

                            out.print("<xmp>"+log+"</xmp>");
                        %>
                    </div>
                </div>
            </div>
            <div class="col-md-7">
                <h4 class="text-center">-- Results --</h4>
                <%
                    try {
                        ArrayList<FullQuery> allQueryList = (ArrayList<FullQuery>) request.getAttribute("allQueryList");

                        for (int queryIdx = 0; queryIdx < allQueryList.size(); queryIdx++) {
                            try {
                                FullQuery curFullQuery = allQueryList.get(queryIdx);
                                out.print("<table class=\"table table-striped table-bordered table-responsive\">");
                                out.print("<thead><tr>");
                                for (int j = 0; j < curFullQuery.getResultList().get(0).length; j++) {
                                    out.print("<th>" + curFullQuery.getIDVarMap().get(j) + "</th>");
                                }
                                out.print("</tr></thead>");
                                out.print("<tbody><tr>");
                                for (int i = 0; i < curFullQuery.getResultList().size(); i++) {
                                    for (int j = 0; j < curFullQuery.getResultList().get(i).length; j++) {
                                        out.print("<th>" + curFullQuery.getResultList().get(i)[j] + "</th>");
                                    }
                                    out.print("</tr>");
                                }
                                out.println("</tbody>");
                                out.println("<p class='text-center'>==== " + curFullQuery.getResultList().size() + " results for query " + (queryIdx + 1) + " ====</p>");
                            } catch (Exception e) {
                                out.print("<p class='text-center'>==== No Result for query " + (queryIdx + 1) + " ====</p>");
                            }
                        }
                    }catch (Exception e) {
                        out.print("<p class='text-center'>==== No Result ====</p>");
                    }
                %>
            </div>
        </div>
    </div>
</body>
</html>
