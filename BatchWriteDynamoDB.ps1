# https://awscli.amazonaws.com/v2/documentation/api/2.4.19/reference/dynamodb/batch-write-item.html
Remove-Variable * -ErrorAction SilentlyContinue
Add-Type -AssemblyName System.Web.Extensions

# Batch Size limit is 25. Source files have been sliced 
$batchSize = 25
$startBatch = 1 # The beginning batch number
$tableName = Read-Host -Prompt 'Input your DynamoDB table name' #Example "NIST80053R5"
$jsonFilePath = Read-Host -Prompt 'Enter the JSON filename path that is being imported' #Example  "F:\dynamodb_batch.json"
$saveFilePath = Read-Host -Prompt 'Enter the path batched DynamoDB JSON files should be stored.' #Example "F:\"
$awsProfileName = Read-Host -Prompt 'Enter an AWS CLI profile name.' # "vaProd"
# Load items from JSON file
$jsonString = [System.IO.File]::ReadAllText($jsonFilePath)
$jsonItems = [System.Web.Script.Serialization.JavaScriptSerializer]::new().DeserializeObject($jsonString)
# Split items into batches
$batches = @()
$batchCount = [Math]::Ceiling($jsonItems.Count/$batchSize)
Write-Host "Number of batches: $($batchCount)"
# Seperate main JSON into batches no greater than 25 requests
for ($i = 0; $i -lt $batchCount; $i++) {
    $batchStart = $i * $batchSize
    $batchEnd = [Math]::Min(($i + 1) * $batchSize, $jsonItems.Count)
    $batch = $jsonItems[$batchStart..($batchEnd - 1)]
    $batches += $batch
}
# Object to contain DynamoDB JSON
$dbTableObject = [PSCustomObject]@{
    "$("$tableName")"= @()
}
# Loop through JSON batches and build DynamoDB JSON Schema needed
for($i=$startBatch; $i -le $batchCount; $i+=1){
    $json = $batches[$i]
    ForEach($items in $json){
        $item = [PSCustomObject]@{
            ID = @{
                S = $items.ID
            }
            Control_Name = @{
                S = $itemsame
            }
            Control_ID = @{
                S = $items.Control_ID
            }
            Control_Enhancement_Name = @{
                S = $items.Control_Enhancement_Name
            }
            Control_Enhancement =  @{
                S = $items.Control_Enhancement
            }
            Discussion =  @{
                S = $items.Discussion
            }
            Related_Controls =  @{
                S = $items.Related_Controls
            }
        }
        # Encapsolate item JSON into Request JSON
        $putRequest = [PSCustomObject]@{
            PutRequest = @{ 
                Item = $item 
            }
        }
        # Delete Request
        <#
        $deleteRequest = [PSCustomObject]@{
            DeleteRequest = @{ 
                Key = $item 
            }
        }
        #>
        $dbTableObject."$tableName" += $putRequest
    }
    $fileName = $saveFilePath + 'batch' + $($i) + '.json' #
    $batchDynamoDBJson = ConvertTo-Json -InputObject $dbTableObject -Depth 6
    $Utf8NoBomEncoding = New-Object System.Text.UTF8Encoding $False
    # Error parsing parameter '--request-items': Expected: '=', received: 'ÿ' for input
    # Disabling BOM removes the tag
    [System.IO.File]::WriteAllLines($fileName, $batchDynamoDBJson, $Utf8NoBomEncoding)
    Write-Host "Sending batch $($i) to DynamoDB via AWS CLI using profile: $($awsProfileName) "
    #aws dynamodb batch-write-item --request-items file://$fileName --profile vaProd
    $dbTableObject."$tableName" = @()
    Remove-Variable $item -ErrorAction SilentlyContinue
    Remove-Variable $putRequest -ErrorAction SilentlyContinue
}
Remove-Variable * -ErrorAction SilentlyContinue