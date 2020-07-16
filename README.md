# AzureFunction.LogAnalytics.Exporter

**Build Status**

[![Build Status](https://dev.azure.com/rabickel/Azure.Functions.LAContinuousExport/_apis/build/status/rbickel.AzureFunction.LogAnalytics.Exporter?branchName=dev)](https://dev.azure.com/rabickel/Azure.Functions.LAContinuousExport/_build/latest?definitionId=39&branchName=dev)

[![Deployment Status](https://vsrm.dev.azure.com/rabickel/_apis/public/Release/badge/eeb388af-a63e-4932-ad76-689cbe58c430/1/1)](https://vsrm.dev.azure.com/rabickel/_apis/public/Release/badge/eeb388af-a63e-4932-ad76-689cbe58c430/1/1)



**One-click deploy to Azure**

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Frbickel%2FAzureFunction.LogAnalytics.Exporter%2Fdev%2Fazuredeploy.json)

## Post deployment tasks

**_NOTE:_**  After deployment, assign the role 'Log Analytics Reader' to the function system assigned identity on the Log Analytic workspace, and restart the function app

## Known issues ##

1. If the functions are lagging far behind, there could be hundred of thousands of log to process. In this case the API calls to LA will probably be throttled. see [https://dev.loganalytics.io/documentation/Using-the-API/Limits](https://dev.loganalytics.io/documentation/Using-the-API/Limits). This is not handled in the current version, but the solution should be able self-heal in many cases. 
