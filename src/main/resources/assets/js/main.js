var getJSON = function(url) {
  return new Promise(function(resolve, reject) {
    var xhr = new XMLHttpRequest();
    xhr.open('get', url, true);
    xhr.responseType = 'json';
    xhr.onload = function() {
      var status = xhr.status;
      if (status == 200) {
        resolve(xhr.response);
      } else {
        reject(status);
      }
    };
    xhr.send();
  });
};


function loadData(slices) {

    getJSON(window.location.pathname+'/ring/'+slices).then(function(result) {
        tokens = result.tokens;
        size = tokens.length;
        data = [];
        var min = 0;
        var max = 1;
        for(i=0; i<size; i++) {
            item = tokens[i];
            for (j=0; j<item.ranges.length; j++) {
                range = item.ranges[j];
                //range.repaired = Math.floor(Math.random() * (max - min + 1)) + min;
                range.repaired = i*size+j;
                data.push(range);
            }
        }
        makeChart(data, size);
    }, function(status) { //error detection....
      console.log('Something went wrong.');
    });

}

function makeChart(data, numTokens) {
    var chart = circularHeatChart()
        .innerRadius(0)
        .segmentHeight(30)
        .numSegments(numTokens)
        .range(["white", "green"]);

    /* Define an accessor function */
    chart.accessor(function(d) {return d.repaired;})

    d3.select('#chart4')
        .selectAll('svg')
        .data([data])
        .enter()
        .append('svg')
        .call(chart);

    /* Add a mouseover event */
    d3.selectAll("#chart4 path").on('mouseover', function() {
        var d = d3.select(this).data()[0];
        console.log(d.start + ':' + d.end);
    });
}

loadData(5);
