package com.ult.temp.producer

import java.util.{Date, Random, UUID}

class TemperatureDevice {

  var deviceId : UUID = UUID.randomUUID()
  var temperature  : Int = 0
  var latitude  : Double = 0
  var longitude : Double = 0
  var location : Map[String,Double] = Map.empty
  var time : Long = 0L

def  createDevice():TemperatureDevice = {
  return this
}
   def genarateRecord():TemperatureDevice={

     var rand = new Random
     this.temperature= rand.nextInt(40 - 10) + 10
     this.latitude= rand.nextDouble() * -180.0 + 90.0
     this.longitude=rand.nextDouble() * -360.0 + 180.0
     this.location=Map(("latitude", this.latitude), ("longitude", this.longitude))
     this.time = new Date().getTime
     return this

   }


}


