import json
import boto3
import logging

dynamodb = boto3.resource("dynamodb")
stepFunction = boto3.client("stepfunctions")
bookTable = dynamodb.Table("Books")
userTable = dynamodb.Table("Users")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class BookNotFoundException(Exception):
    pass


class BookOuOfStockException(Exception):
    pass


def checkBookAvailable(book, qty):
    if book["qty"] - qty > 0:
        return True
    else:
        return False


def checkInventory(event, context):
    bookId = event["bookId"]
    qty = event["qty"]
    response = bookTable.get_item(Key={"bookId": str(bookId)})
    if "Item" not in response:
        raise BookNotFoundException("Book not found")
    else:
        book = response["Item"]
        isBookAvailable = checkBookAvailable(book, qty)
        if isBookAvailable == True:
            return book
        else:
            raise BookOuOfStockException("Book out of stock")


def calculateTotal(event, context):
    price = event["book"]["price"]
    qty = event["qty"]
    total = price * qty
    return {"total": total}


def deductPoints(userId):
    userTable.update_item(
        Key={"userId": str(userId)},
        UpdateExpression="SET points = :n",
        ExpressionAttributeValues={
            ":n": 0,
        },
    )


def redeemPoints(event, context):
    orderTotal = event["total"]["total"]
    userId = event["userId"]
    response = userTable.get_item(Key={"userId": str(userId)})
    logger.info(response)
    points = response["Item"]["points"]
    if orderTotal > points:
        deductPoints(userId)
        orderTotal = orderTotal - points
        return {"total": orderTotal, "points": points}
    else:
        raise Exception("Order is less than redeem points!")


def billCustomer(event, context):
    return "Successfully Billed the Customer!"


def restoreRedeemPoints(event, context):
    points = event["total"]["points"]
    userId = event["userId"]
    response = userTable.update_item(
        Key={"userId": str(userId)},
        UpdateExpression="SET points = :n",
        ExpressionAttributeValues={
            ":n": points,
        },
    )
    logger.info(response)


def updateBookQty(bookId, qty):
    bookTable.update_item(
        Key={"bookId": str(bookId)},
        UpdateExpression="SET qty = qty - :n",
        ExpressionAttributeValues={
            ":n": qty,
        },
    )


def restoreQuantity(event, context):
    logger.info(event)
    bookId = event["bookId"]
    qty = event["qty"]
    bookTable.update_item(
        Key={"bookId": str(bookId)},
        UpdateExpression="SET qty = qty + :n",
        ExpressionAttributeValues={
            ":n": qty,
        },
    )


def sqsWorker(event, context):
    body = json.loads(event["Records"][0]["body"])
    logger.info(body)
    courier = "aiswarya96.rajaponnan@gmail.com"
    updateBookQty(body["Input"]["bookId"], body["Input"]["qty"])
    try:
        stepFunction.send_task_success(
            taskToken=body["Token"], output=json.dumps({"courier": courier})
        )
    except Exception as e:
        logger.info(e)
        stepFunction.send_task_failure(
            taskToken=body["Token"],
            cause="Courier not available!",
            error="NoCourierAvailable",
        )
