<%@ page import="java.util.Scanner" %>
<%@ page import="java.io.IOException" %>
<%@ page import="java.io.File" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html lang="en">

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
    <form action="queryServlet" method="post" class="form-group">
        <div class="col-lg-12 text-center form-group" id="TextBoxesGroup">
            <div id="TextBoxDiv1">
                <textarea id="textbox1" name="query1" class="form-control" rows="5" placeholder="Please Input your SPARQL query here" ></textarea>
            </div>
        </div>

        <div class="col-lg-12 form-group">
            <span class="col-md-4">
                <button type="submit" class="btn btn-primary" id="submit">Search</button>
            </span>
            <span class="col-md-4 border-left">
                <button type="button" class="btn btn-success btn-sm" id="addButton"> + Add</button>
            </span>
            <span class="col-md-4 border-left">
                <button type="button" class="btn btn-success btn-sm" id="removeButton"> - Remove</button>
            </span>
        </div>
        <div class="col-lg-12 form-group">
            <span class="col-md-3">
                <button type="button" class="btn btn-info btn-sm" id="lifeExpBu">Example_lifeScience</button>
            </span>
            <span class="col-md-3 border-left">
                <button type="button" class="btn btn-info btn-sm" id="crossExpBu">Example_crossDomain</button>
            </span>
            <span class="col-md-3 border-left">
                <button type="button" class="btn btn-info btn-sm" id="largeExpBu">Example_largeRDFData</button>
            </span>
            <span class="col-md-3 border-left">
                <button type="button" class="btn btn-info btn-sm" id="watDivExpBu">Example_watDiv100M</button>
            </span>
        </div>
        <div class="col-lg-12 form-group">
            <div class="form-check form-check-inline">
                <label class="form-check-label">
                    <input type="checkbox" name="config" class="form-check-input" checked value="0">LifeScience
                </label>
            </div>
            <div class="form-check form-check-inline">
                <label class="form-check-label">
                    <input type="checkbox" name="config" class="form-check-input" value="1">CrossDomain
                </label>
            </div>
            <div class="form-check form-check-inline">
                <label class="form-check-label">
                    <input type="checkbox" name="config" class="form-check-input" value="2">LargeRDFData
                </label>
            </div>
            <div class="form-check form-check-inline">
                <label class="form-check-label">
                    <input type="checkbox" name="config" class="form-check-input" value="3">WatDiv100M
                </label>
            </div>
        </div>

    </form>
</div>
<%
    String path = request.getRealPath("");
    StringBuilder result = new StringBuilder();
    try (Scanner scanner = new Scanner(new File(path + "/WEB-INF/queryExamples.txt"))) {

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            result.append(line).append("\n");
        }

    } catch (IOException e) {
        e.printStackTrace();
    }
%>
<xmp id="lifeExp1" style="display: none"><%=result.toString().split("\n")[0].split("=")[1]%></xmp>
<xmp id="crossExp1" style="display: none"><%=result.toString().split("\n")[1].split("=")[1]%></xmp>
<xmp id="largeExp1" style="display: none"><%=result.toString().split("\n")[2].split("=")[1]%></xmp>
<xmp id="watDivExp1" style="display: none"><%=result.toString().split("\n")[3].split("=")[1]%></xmp>
<xmp id="lifeExp2" style="display: none"><%=result.toString().split("\n")[4].split("=")[1]%></xmp>
<xmp id="crossExp2" style="display: none"><%=result.toString().split("\n")[5].split("=")[1]%></xmp>
<xmp id="largeExp2" style="display: none"><%=result.toString().split("\n")[6].split("=")[1]%></xmp>
<xmp id="watDivExp2" style="display: none"><%=result.toString().split("\n")[7].split("=")[1]%></xmp>
<script>

    var counter = 2;

    function addTextBox() {
        if(counter>10){
            alert("Only 10 textboxes allow");
            return false;
        }

        var newTextBoxDiv = $(document.createElement('div'))
            .attr("id", 'TextBoxDiv' + counter);

        newTextBoxDiv.after().html('<br/>'+
            '<textarea class="form-control" rows="5" name="query' + counter +
            '" id="textbox' + counter + '" placeholder="Please Input your SPARQL query here" >');

        newTextBoxDiv.appendTo("#TextBoxesGroup");

        counter++;
    }

    function removeTextBox() {
        if(counter===2) {
            alert("No more textbox to remove");
            return false;
        }

        counter--;

        $("#TextBoxDiv" + counter).remove();
    }

    $("#submit").click(function() {
       if ($("#textbox" + (counter - 1)).val() === "") {
           event.preventDefault();
           alert("Please Input a Query for textbox" + (counter-1) + " !");
       }
    });

    $("#addButton").click(function() {
        addTextBox()
    });

    $("#removeButton").click(function() {
        removeTextBox()
    });

    var times1 = 1,
        times2 = 1,
        times3 = 1,
        times4 = 1;

    $("#lifeExpBu").click(function () {
        if (times1 <= 2) {
            $("#textbox" + (counter - 1)).val($("#lifeExp" + times1).text());
            times1++;
        }
        if (times1 === 3) {
            $("#lifeExpBu").attr("class","btn-secondary").prop('disabled', true);
        }
        console.log(times1);
    });
    $("#crossExpBu").click(function () {
        if (times2 <= 2) {
            $("#textbox" + (counter - 1)).val($("#crossExp" + times2).text());
            times2++;
        }
        if (times2 === 3) {
            $("#crossExpBu").attr("class","btn-secondary").prop('disabled', true);
        }
    });
    $("#largeExpBu").click(function () {
        if (times3 <= 2) {
            $("#textbox" + (counter - 1)).val($("#largeExp" + times3).text());
            times3++;
        }
        if (times3 === 3) {
            $("#largeExpBu").attr("class","btn-secondary").prop('disabled', true);
        }
    });
    $("#watDivExpBu").click(function () {
        if (times4 <= 2) {
            $("#textbox" + (counter - 1)).val($("#watDivExp" + times4).text());
            times4++;
        }
        if (times4 === 3) {
            $("#watDivExpBu").attr("class","btn-secondary").prop('disabled', true);
        }
    });

</script>
</body>

</html>