from boto3.dynamodb.conditions import Key, Attr

def getCatFilters(request):
    filter_conditions = []
    if 'categories' in request:
        for cat in request['categories']:
            if filter_conditions == []:
                filter_conditions = Attr('category').eq(cat)
            else:
                filter_conditions = filter_conditions | Attr('category').eq(cat)
    return filter_conditions

def getRequirementFilters(request):
    filter_conditions = []
    if 'requirements' in request:
        for requirement in request['requirements']:
            if filter_conditions == []:
                filter_conditions = Attr('requirements').contains(requirement)
            else:
                filter_conditions = filter_conditions | Attr('requirements').contains(requirement)
    return filter_conditions