package comp.bio.aging.crispr.server

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.{HttpApp, Route}

// Server definition
object WebServer extends HttpApp {
  def route: Route =
    path("hello") {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
      }
    }
}


object App extends scala.App{
  // Starting the server
  WebServer.startServer("localhost", 8080)
}

