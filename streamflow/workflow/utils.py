from typing import Any, Iterable, MutableSequence, Type, Union

from streamflow.core.workflow import Token
from streamflow.cwl.token import CWLFileToken
from streamflow.workflow.token import (
    IterationTerminationToken,
    ListToken,
    ObjectToken,
    TerminationToken, JobToken,
)


def check_iteration_termination(inputs: Union[Token, Iterable[Token]]) -> bool:
    return check_token_class(inputs, IterationTerminationToken)


def check_termination(inputs: Union[Token, Iterable[Token]]) -> bool:
    return check_token_class(inputs, TerminationToken)


def check_token_class(inputs: Union[Token, Iterable[Token]], cls: Type[Token]):
    if isinstance(inputs, Token):
        return isinstance(inputs, cls)
    else:
        for token in inputs:
            if isinstance(token, MutableSequence):
                if check_token_class(token, cls):
                    return True
            elif isinstance(token, cls):
                return True
        return False


def get_token_value(token: Token) -> Any:
    if isinstance(token, ListToken):
        return [get_token_value(t) for t in token.value]
    elif isinstance(token, ObjectToken):
        return {k: get_token_value(v) for k, v in token.value.items()}
    elif isinstance(token.value, Token):
        return get_token_value(token.value)
    else:
        return token.value


def contains_file(token: Token) -> bool:
    if isinstance(token, CWLFileToken):
        return True
    elif isinstance(token, ListToken):
        return any([ contains_file(t) for t in token.value ])
    elif isinstance(token, ObjectToken):
        return contains_file(token.value)
    elif isinstance(token, JobToken) or isinstance(token, TerminationToken) or isinstance(token, IterationTerminationToken):
        return False
    elif isinstance(token, Token):
        return contains_file(token.value)
    else:
        return isinstance(token, dict) and 'class' in token and (token['class'] == 'File' or token['class'] == 'Directory')



def get_file(token: Token) -> MutableSequence[dict]:
    if isinstance(token, CWLFileToken):
        return [token.value]
    elif isinstance(token, ListToken):
        return [ inner_t for outer_t in token.value for inner_t in get_file(outer_t)]
    elif isinstance(token, ObjectToken):
        return get_file(token.value)
    elif isinstance(token, JobToken) or isinstance(token, TerminationToken) or isinstance(token, IterationTerminationToken):
        return []
    elif isinstance(token, Token):
        return get_file(token.value)
    else:
        if isinstance(token, dict) and 'class' in token and (token['class'] == 'File' or token['class'] == 'Directory'):
            return [token]
        else:
            return []
