#! /bin/bash
awslocal dynamodb create-table --table-name webasto-readings \
    --key-schema AttributeName=timestamp,KeyType=HASH \
    --attribute-definitions AttributeName=timestamp,AttributeType=N \
    --billing-mode PAY_PER_REQUEST --region us-east-2
