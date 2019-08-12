siddhi-execution-timeseries
======================================

The **siddhi-execution-timeseries extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> 
which enables users to forecast and detect outliers in time series data, using Linear Regression Models.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-timeseries">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-timeseries/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-timeseries/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-timeseries/api/5.0.1">5.0.1</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this 
extension you can replace the component <a target="_blank" href="https://github
.com/wso2-extensions/siddhi-execution-timeseries/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.execution.timeseries</groupId>
        <artifactId>siddhi-execution-timeseries</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-timeseries/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-timeseries/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-timeseries/api/5.0.1/#forecast-stream-processor">forecast</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>This allows the user to specify a batch size (optional) that defines the number of events to be considered for the regression calculation when forecasting the Y value.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-timeseries/api/5.0.1/#kalmanminmax-stream-processor">kalmanMinMax</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>The kalmanMinMax function uses the kalman filter to smooth the values of the time series within a given window, and then determine the maxima and minima of that set of values.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-timeseries/api/5.0.1/#kernelminmax-stream-processor">kernelMinMax</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>TBD</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-timeseries/api/5.0.1/#lengthtimeforecast-stream-processor">lengthTimeForecast</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>This allows the user to restrict the number of events considered for the regression calculation when forecasting the Y value based on a specified time window and/or batch size.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-timeseries/api/5.0.1/#lengthtimeoutlier-stream-processor">lengthTimeOutlier</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>This allows you to restrict the number of events considered for the regression calculation performed when finding outliers based on a specified time window and/or a batch size.<br>This function should be used in one of the following formats.<br>lengthTimeOutlier(time window, batch size, range, Y, X) OR<br>lengthTimeOutlier(time window, batch size, range, calculation interval, confidence interval, Y, X)<br>There can be different outputs and β coefficients of the regression equation and can return dynamic attributes as beta1 , beta2 ... betan.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-timeseries/api/5.0.1/#lengthtimeregress-stream-processor">lengthTimeRegress</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>This allows the user to specify the time window and batch size (required). The number of events considered for the regression calculation can be restricted based on the time window and/or the batch size.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-timeseries/api/5.0.1/#outlier-stream-processor">outlier</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>This allows the user to specify a batch size (optional) that defines the number of events to be considered for the calculation of regression while finding the outliers.<br>This function should be used in one of the following formats.<br>outlier(range, Y, X)<br>or<br>outlier(calculation interval, batch size, confidence interval, range, Y, X). There can be different outputs and β coefficients of the regression equation and can return dynamic attributes as beta1 , beta2 ... betan.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-timeseries/api/5.0.1/#regress-stream-processor">regress</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p>This allows the user to specify the batch size (optional) that defines the number of events to be considered for the calculation of regression.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-timeseries/issues">GitHub 
  Issue
   Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github
  .com/wso2-extensions/siddhi-execution-timeseries/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
