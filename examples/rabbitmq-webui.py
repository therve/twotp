# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Example of a web server interacting with RabbitMQ.
"""

import sys
from urllib import quote

from Cheetah.Template import Template

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.static import Data
from twisted.python import log

from twotp import Process, readCookie, buildNodeName
from twotp.term import Binary, Atom


css = """
body {
    font-family: sans-serif;
    font-size: 75%;
    margin: 0;
    padding: 0;
    background-color: #EEEEEE;
}

table, tr, td
{
    border-collapse: collapse;
    margin: 0 1em 0 1em;
    padding: 0;
}

thead {
    font-weight: bold;
    text-align: center;
}

td {
    border: 1px solid #666666;
    padding: 0.1em 0.5em 0.1em 0.5em;
}

#content {
    width: 80%;
    height: 100%;
    margin: auto;
    border-left: 1px solid black;
    border-right: 1px solid black;
    background-color: #FFFFFF;
}

h1 {
    margin: 0;
    padding: 1em 0 0 1em;
}

h3 {
    margin: 0;
    padding: 1em;
}

form.out_form {
    padding: 1em 0 0 1em;
}

form.in_form {
    padding: 0.1em 0 0.1em 0;
    margin: 0;
}

form.in_form input {
    margin: 0;
}
"""


summary = """<html>
<head>
    <title>RabbitMQ TwOTP Web UI</title>
    <link type="text/css" href="/style.css" rel="stylesheet">
</head>
<body>
    <div id="content">
    <h1>RabbitMQ management</h1>
    <h3>Virtual Hosts<h3>
    <table>
    <thead>
        <tr>
            <td>Name</td>
            <td>Delete</td>
        </tr>
    </thead>
    <tbody>
        #for $vhost in $vhosts
        <tr>
            <td><a href="vhost/$quote($vhost.value, safe="")">$vhost.value</a></td>
            <td><a href="vhost/$quote($vhost.value, safe="")/delete">Delete</a></td>
        </tr>
        #end for
    </tbody>
    </table>
    <form class="out_form" name="add_vhost" method="POST" action="add_vhost">
        <input type="text" name="vhost" />
        <input type="submit" value="Add" />
    </form>
    <h3>Users<h3>
    <table>
    <thead>
        <tr>
            <td>Name</td>
            <td>Virtual hosts</td>
            <td>Map to vhost</td>
            <td>Delete</td>
        </tr>
    </thead>
    <tbody>
        #for $user in $users
        <tr>
            <td>$user.value</td>
            <td>$user_vhosts[$user]</td>
            <td>
                <form class="in_form" name="map_user_vhost" method="POST"
                      action="user/$quote($user.value, safe="")/map_vhost">
                    <input type="text" name="vhost" />
                    <input type="submit" value="Map" />
                </form>
            </td>
            <td><a href="user/$quote($user.value, safe="")/delete">Delete</a></td>
        </tr>
        #end for
    </tbody>
    </table>
    <form class="out_form" name="add_user" method="POST" action="add_user">
        <input type="text" name="user" />
        <input type="password" name="password" />
        <input type="submit" value="Add" />
    </form>
    </div>
</body>
</html>
"""

vhost = """<html>
<head>
    <title>RabbitMQ TwOTP Web UI</title>
    <link type="text/css" href="/style.css" rel="stylesheet">
</head>
<body>
    <div id="content">
    <h1>Virtual host '$vhost'</h1>
    <h3>List of exchanges</h3>
    <table>
    <thead>
        <tr>
            <td>Name</td>
            <td>Type</td>
            <td>Durable</td>
            <td>Auto-delete</td>
        </tr>
    </thead>
    <tbody>
        #for $exchange in $exchanges
        <tr>
            <td>$exchange[0]</td>
            <td>$exchange[1]</td>
            <td>$exchange[2]</td>
            <td>$exchange[3]</td>
        </tr>
        #end for
    </tbody>
    </table>
    <h3>List of queues</h3>
    <table>
    <thead>
        <tr>
            <td>Name</td>
            <td>Durable</td>
            <td>Auto-delete</td>
            <td>Total messages</td>
            <td>Memory</td>
            <td>Consumers</td>
        </tr>
    </thead>
    <tbody>
        #for $queue in $queues
        <tr>
            <td>$queue[0]</td>
            <td>$queue[1]</td>
            <td>$queue[2]</td>
            <td>$queue[8]</td>
            <td>$queue[12]</td>
            <td>$queue[10]</td>
        </tr>
        #end for
    </tbody>
    </table>
    </div>
</body>
</html>
"""


class VhostDeleteResource(Resource):
    isLeaf = True

    def __init__(self, process, vhost):
        Resource.__init__(self)
        self.process = process
        self.vhost = vhost

    def render_GET(self, request):
        def deleted(result):
            request.redirect("/")
            request.finish()
        process.callRemote(
            "rabbit", "rabbit_access_control", "delete_vhost",
            Binary(self.vhost)).addCallback(deleted)
        return NOT_DONE_YET


class VhostAddResource(Resource):
    isLeaf = True

    def __init__(self, process):
        Resource.__init__(self)
        self.process = process

    def render_POST(self, request):
        vhost = request.args["vhost"][0]
        def added(result):
            request.redirect("/")
            request.finish()
        process.callRemote(
            "rabbit", "rabbit_access_control", "add_vhost",
            Binary(vhost)).addCallback(added)
        return NOT_DONE_YET


class VhostResource(Resource):

    def __init__(self, process, vhost):
        Resource.__init__(self)
        self.process = process
        self.vhost = vhost

    def getChild(self, path, request):
        if path == "delete":
            return VhostDeleteResource(self.process, self.vhost)
        return self

    @inlineCallbacks
    def get_infos(self):
        items = [
            "name", "durable", "auto_delete", "arguments", "pid",
            "messages_ready", "messages_unacknowledged",
            "messages_uncommitted", "messages", "acks_uncommitted",
            "consumers", "transactions", "memory"]
        items = [Atom(item) for item in items]
        queues = yield process.callRemote(
            "rabbit", "rabbit_amqqueue", "info_all", Binary(self.vhost), items)
        queues = [
            (queue[0][1][3].value,
             queue[1][1].text == "true",
             queue[2][1].text == "true",
             queue[3][1],
             queue[4][1].nodeName.text,
             queue[5][1],
             queue[6][1],
             queue[7][1],
             queue[8][1],
             queue[9][1],
             queue[10][1],
             queue[11][1],
             queue[12][1])
            for queue in queues]

        items = ["name", "type", "durable", "auto_delete", "arguments"]
        items = [Atom(item) for item in items]
        exchanges = yield process.callRemote(
            "rabbit", "rabbit_exchange", "info_all", Binary(self.vhost), items)
        exchanges = [
            (exchange[0][1][3].value,
             exchange[1][1].text,
             exchange[2][1].text == "true",
             exchange[3][1].text == "true",
             exchange[4][1])
            for exchange in exchanges]

        returnValue((queues, exchanges))

    def render_GET(self, request):
        def got_result(results):
            queues = results[0]
            exchanges = results[1]
            template = Template(vhost)
            template.queues = queues
            template.exchanges = exchanges
            template.vhost = self.vhost
            request.write(str(template))
            request.finish()
        self.get_infos().addCallback(got_result)
        return NOT_DONE_YET


class ListVhostsResource(Resource):

    def __init__(self, process):
        Resource.__init__(self)
        self.process = process

    def getChild(self, path, request):
        return VhostResource(self.process, path)


class UserDeleteResource(Resource):
    isLeaf = True

    def __init__(self, process, user):
        Resource.__init__(self)
        self.process = process
        self.user = user

    def render_GET(self, request):
        def deleted(result):
            request.redirect("/")
            request.finish()
        process.callRemote(
            "rabbit", "rabbit_access_control", "delete_user",
            Binary(self.user)).addCallback(deleted)
        return NOT_DONE_YET


class UserMapVhostResource(Resource):
    isLeaf = True

    def __init__(self, process, user):
        Resource.__init__(self)
        self.process = process
        self.user = user

    def render_POST(self, request):
        vhost = request.args["vhost"][0]
        def mapped(result):
            request.redirect("/")
            request.finish()
        process.callRemote(
            "rabbit", "rabbit_access_control", "map_user_vhost",
            Binary(self.user), Binary(vhost)).addCallback(mapped)
        return NOT_DONE_YET


class UserAddResource(Resource):
    isLeaf = True

    def __init__(self, process):
        Resource.__init__(self)
        self.process = process

    def render_POST(self, request):
        user = request.args["user"][0]
        password = request.args["password"][0]
        def added(result):
            request.redirect("/")
            request.finish()
        process.callRemote(
            "rabbit", "rabbit_access_control", "add_user",
            Binary(user), Binary(password)).addCallback(added)
        return NOT_DONE_YET


class UserResource(Resource):

    def __init__(self, process, user):
        Resource.__init__(self)
        self.process = process
        self.user = user

    def getChild(self, path, request):
        if path == "delete":
            return UserDeleteResource(self.process, self.user)
        elif path == "map_vhost":
            return UserMapVhostResource(self.process, self.user)
        return self


class ListUsersResource(Resource):

    def __init__(self, process):
        Resource.__init__(self)
        self.process = process

    def getChild(self, path, request):
        return UserResource(self.process, path)


class WebUi(Resource):

    css = Data(css, "text/css")

    def __init__(self, process):
        Resource.__init__(self)
        self.process = process

    @inlineCallbacks
    def get_infos(self):
        vhosts = yield process.callRemote(
            "rabbit", "rabbit_access_control", "list_vhosts")
        users = yield process.callRemote(
            "rabbit", "rabbit_access_control", "list_users")
        mapping = {}
        for user in users:
            user_vhosts = yield process.callRemote(
                "rabbit", "rabbit_access_control", "list_user_vhosts", user)
            mapping[user] = user_vhosts
        returnValue((vhosts, users, mapping))

    def getChild(self, path, request):
        if path == "vhost":
            return ListVhostsResource(self.process)
        elif path == "add_vhost":
            return VhostAddResource(self.process)
        if path == "user":
            return ListUsersResource(self.process)
        elif path == "add_user":
            return UserAddResource(self.process)
        elif path == "style.css":
            return self.css
        return self

    def render_GET(self, request):
        def got_result(results):
            vhosts = results[0]
            users = results[1]
            accesses = results[2]
            for user, user_vhosts in accesses.iteritems():
                accesses[user] = ", ".join(v.value for v in user_vhosts)
            template = Template(summary)
            template.vhosts = vhosts
            template.users = users
            template.user_vhosts = accesses
            template.quote = quote
            request.write(str(template))
            request.finish()
        self.get_infos().addCallback(got_result)
        return NOT_DONE_YET


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    cookie = readCookie()
    nodeName = buildNodeName("twotp-rabbit")
    process = Process(nodeName, cookie)
    reactor.listenTCP(8072, Site(WebUi(process)))
    reactor.run()
