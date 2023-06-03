from viva.core.planner import Planner

Img2VecTree = {
    'val': 'skip',
    'children': [
        {
            'val': 'i2v',
            'children': []
        },
        {
            'val': 'ad',
            'children': []
        }
    ]
}

Img2VecPlan = Planner(Img2VecTree)
