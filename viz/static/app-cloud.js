// D3 Word Cloud Implementation by Eric Coopey:
// http://bl.ocks.org/ericcoopey/6382449

var sourcestockvalue = new EventSource('/streamcompanystockvalue');
var sourcewordcount =new EventSource('/streamcompanywordcount');
var wordhash = {};
var pricehash ={};
var width = 1200;
var height = 700;
var newvar="hello";
//update hash (associative array) with incoming word and count
sourcestockvalue.onmessage = function (event) {
  company = event.data.split("|")[0];
  price = event.data.split("|")[1];
  predictedprice = event.data.split("|")[2];
  sentiment = event.data.split("|")[3];
  if(!skip(word)){
    if(!pricehash[company]){
      pricehash[company]={};
    }
    if(!pricehash[company]["value"]){
      pricehash[company]["value"]=[];
    }
    if(!pricehash[company]["predictedvalue"]){
      pricehash[company]["predictedvalue"]=[];
    }
    pricehash[company]["value"].push(price);
    pricehash[company]["predictedvalue"].push(predictedprice);
    pricehash[company]["sentiment"]=sentiment;
  }
};
sourcewordcount.onmessage = function (event) {
  company = event.data.split("|")[0];
  word = event.data.split("|")[1];
  count = event.data.split("|")[2];
  if(!skip(word)){
    if(!wordhash[company]){
      wordhash[company]={};
      wordhash[company][word]=count;
    }
  }
};
//update function for visualization


//clean list, can be added to word skipping bolt
var skipList = ["https","follow","1","2","please","following","followers","fucking","RT","the","at","a"];

var skip = function(tWord){
  for(var i=0; i<skipList.length; i++){
    if(tWord === skipList[i]){
      return true;
    }
  }
  return false;
};
