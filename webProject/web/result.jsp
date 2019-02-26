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

    <title>Bare - Start Bootstrap Template</title>

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
                    <a class="nav-link" href="#">About</a>
                </li>
                <li class="nav-item">
                    <a class="nav-link" href="#">Services</a>
                </li>
                <li class="nav-item">
                    <a class="nav-link" href="#">Contact</a>
                </li>
            </ul>
        </div>
    </div>
</nav>
    <div class="container">
        <a href="javascript:history.go(-1)">Back</a>
    </div>
    <div class="container">
        <h2 style="text-align: center">----- SPARQL Queries -----</h2>
        <%
            String[] queries = queryServlet.getQueries();
            int queryIdx = Integer.parseInt(request.getParameter("queryIdx")) - 1;

            out.println("query " + queryIdx + ":<br/>");
            out.println(queries[queryIdx] + "<br/><br/>");

        %>

        <h2 style="text-align: center">----- FMQO -----</h2>
        <%
            ArrayList<FullQuery> allQueryList = queryServlet.getAllQueryList();

            FullQuery curFullQuery = allQueryList.get(queryIdx);
            out.print("<table class=\"table table-striped table-bordered\">");
            out.print("<thead><tr>");
            for (int j = 0; j < curFullQuery.getResultList().get(0).length; j++) {
                out.print("<th>"+curFullQuery.getIDVarMap().get(j) + "</th>");
            }
            out.print("</tr></thead>");
            out.print("<tbody><tr>");
            for (int i = 0; i < curFullQuery.getResultList().size(); i++) {
                for (int j = 0; j < curFullQuery.getResultList().get(i).length; j++) {
                    out.print("<th>"+curFullQuery.getResultList().get(i)[j] + "</th>");
                }
                out.print("</tr>");
            }
            out.println("</tbody>");
            out.println("==== there are "+ curFullQuery.getResultList().size()+ " results for query " + (queryIdx+1) + " ====");
            out.println("<br/><br/>");

        %>
    </div>

</body>
</html>
