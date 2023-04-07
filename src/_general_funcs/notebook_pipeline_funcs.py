def exit_notebook(notebook_return, fail=True):
    """
    Function exit_notebook to use to pass fail messages/params between notebooks
    
    params:
        notebook_return dict/string: if setting fail to False, pass EITHER string with pass message, OR dictionary with any needed params to pass to next notebook
                                     if keeping fail as True, pass string with error message
        fail bool: optional param to specify failure, default=true
        
    returns:
        dbutils.notebook.exit() with dictionary of return params
    
    """
    
    # if fail is left as default True, return notebook_return as message,
    # pass full value
    
    if fail:
        notebook_return = {'message': notebook_return,
                          'fail': True}
        
    else:
        if type(notebook_return) == str:
            notebook_return = {'message': notebook_return}
            
        notebook_return['fail'] = False
    
    return dbutils.notebook.exit(notebook_return)

def notebook_returns_passthrough(returns_dict, pass_message, fail_message_key='message', bool_fail_key='fail'):
    """
    Function notebook_returns_passthrough to be used on batch notebook to take returns from individual notebook
        with fail boolean and either exit with fail message if failed, or print pass message
        
    params:
        returns_dict dict: dictionary returned from individual notebook with fail boolean and messages to print on pass/fail
        pass_message str: text to print of fail bool == False
        fail_message_key str: optional param to specify name of key with message to print if fail, default = 'message'
        bool_fail_key str: optional param to specify name of key with fail boolean, default = 'fail'
        
    returns:
        print with additional notebook.exit() if failed
    
    """

    if returns_dict[bool_fail_key]:
        dbutils.notebook.exit("NOTEBOOK FAILED:" + '\n\t' + f"{returns_dict[fail_message_key]}")

    else:
        print(pass_message)