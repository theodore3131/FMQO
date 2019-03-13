<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html lang="en">

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

<!-- Page Content -->
<div class="container">
    <div class="row">
        <div class="col-lg-12 text-center">
            <h1 class="mt-5">Federated Multiple Query Optimization</h1>
            <p class="lead">Search with SPARQL Query!</p>
            <ul class="list-unstyled">
                <li>RDF</li>
                <li>SPARQL</li>
            </ul>
        </div>
    </div>
</div>

<div class="container">
    <!--do not use <form> cuz it will cause page redirection which in result cannot show the ajax response.-->
    <div class="col-lg-12 text-center">
        <div class="form-group form-group-lg">
            <textarea name="query" class="form-control" rows="10" placeholder="Please Input your SPARQL query here" ></textarea>
            <br/><br/>
            <span class="input-group-btn">
                <button class="btn btn-primary" id="submit">search</button>
            </span>
        </div>
    </div>
</div>

<div class="container">
    <%--<h2 style="text-align: center">----- SPARQL Queries -----</h2>--%>
    <%--<div id="query"></div>--%>
    <h2 style="text-align: center">----- Result -----</h2>
    <div id="res" style="text-align: center">
        waiting...
    </div>
</div>

<script>
    $('#submit').click(function () {
        $.ajax({
            type: "POST",
            url: "/queryServlet",
            dataType: "text",
            async: false,
            cache: true,
            data: {"query": $("[name=query]").val()},
            success: function(data){
                data = data.split('\n');
                for (var i = 0; i < data.length; i++) {
                    data[i] = "<a href='result.jsp?queryIdx="+ (i+1) + "'>" + data[i] + "</a><br/>";
                }
                $('#res').html(data);
                window.sessionStorage.setItem("myhtml",$("#res").html());
            },
            error: function(){
                console.log("error");
            }
        })
    })
    //页面刷新
    window.onload=function() {
        //读取sessionStorage对象中的内容
        var myhtml = window.sessionStorage.getItem("myhtml");
        //不为空表示是返回上一步进入该页面的。
        if (myhtml != null) {
            //将sessionStorage对象中保存的页面添加到页面中
            $("#res").html(myhtml);
            // //清空sessionStorage对象的内容。
            // window.sessionStorage.clear();
        }
    }
</script>
</body>

</html>