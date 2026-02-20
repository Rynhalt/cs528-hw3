import functions_framework

@functions_framework.http
def file_reader(request):
    return "hello\n", 200