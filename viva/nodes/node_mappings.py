import logging
import os

from viva.nodes.inference_nodes import (
    ObjectDetectNode, ObjectDetectNodeXLarge, ObjectDetectNodeNano,
    ObjectDetectNodeMedium, ObjectDetectNodeLarge, FaceDetectNode,
    EmotionDetectNode, ClassificationNode, QuantizedClassificationNode,
    ObjectTrackNode, ActionDetectNode, ProxyClassificationNode, Img2VecNode,
    TASTIObjectDetectNode, TASTIEmotionDetectNode, TASTIFaceDetectNode,
    TASTIActionDetectNode, EmotionDetectNodeCascades, DeepFaceNode,
    DeepFacePrefixNode, DeepFaceSuffixNode
)
from viva.nodes.miscellaneous_nodes import (
    TranscriptSearchNode, SimilarityNode, BrightnessNode, SVMNode, MotionNode,
    SimpleTransferNode, OverheadNode, ComplexTransferNode, DataGeneratorNode
)
from viva.utils.config import ConfigManager

config = ConfigManager()
deepface_common_prefix_num_layers = config.get_value('execution', 'deepface_common_prefix_num_layers')

logging.warn('Nodes->loading node mappings.')

# Hierarchies are used for plan pruning
objdet_hierarchy = {
    'objectdetect_xlarge': 0,
    'objectdetect_large': 1,
    'objectdetect_medium': 2,
    'objectdetect': 3,
    'objectdetect_nano': 4,
    'objectdetect_tasti': 5
}

emodet_hierarchy = {
    'emotiondetect': 0,
    'emotiondetect_cascades': 1,
    'emotiondetect_tasti': 2
}

facedet_hierarchy = {
    'facedetect': 0,
    'facedetect_tasti': 2
}

union_hierarchy = objdet_hierarchy.copy()
union_hierarchy.update(emodet_hierarchy)
union_hierarchy.update(facedet_hierarchy)

inp_col_list = ['framebytes', 'width', 'height']
tasti_index_pth = os.path.join(config.get_value("storage", "input"), 'tasti_index.bin')

class NodeMap:
    def __init__(self):
        self.NodeMappings = {
            'ts': TranscriptSearchNode([]),
            'si': SimilarityNode(inp_col_list),
            'bi': BrightnessNode(inp_col_list),
            'svm': SVMNode(inp_col_list),
            'md': MotionNode(inp_col_list),
            'fd': (FaceDetectNode, (inp_col_list,)),
            'ed': (EmotionDetectNode, (inp_col_list,)),
            'edc': (EmotionDetectNodeCascades, (inp_col_list,)),
            'cl': (ClassificationNode, (inp_col_list,)),
            'qcl': (QuantizedClassificationNode, (inp_col_list,)),
            'pc': (ProxyClassificationNode, (inp_col_list,)),
            'ad': (ActionDetectNode, (inp_col_list,)),
            'odn': (ObjectDetectNodeNano, (inp_col_list,)),
            'od': (ObjectDetectNode, (inp_col_list,)),
            'odm': (ObjectDetectNodeMedium, (inp_col_list,)),
            'odl': (ObjectDetectNodeLarge, (inp_col_list,)),
            'odx': (ObjectDetectNodeXLarge, (inp_col_list,)),
            'ot': (ObjectTrackNode, (inp_col_list,)),
            'i2v': (Img2VecNode, (inp_col_list,)),
            'tod': TASTIObjectDetectNode(['img2vec'], tasti_index_pth),
            'ted': TASTIEmotionDetectNode(['img2vec'], tasti_index_pth),
            'tfd': TASTIFaceDetectNode(['img2vec'], tasti_index_pth),
            'tad': TASTIActionDetectNode(['img2vec'], tasti_index_pth),
            'dfage': (DeepFaceNode, (inp_col_list, [], 'Age')),
            'dfgender': (DeepFaceNode, (inp_col_list, [], 'Gender')),
            'dfrace': (DeepFaceNode, (inp_col_list, [], 'Race')),
            'dfemo': (DeepFaceNode, (inp_col_list, [], 'Emotion')),
            'dfprefix': (DeepFacePrefixNode, (inp_col_list, [], deepface_common_prefix_num_layers)),
            'dfsuffixage': (DeepFaceSuffixNode, (['dfprefixembed'], [], 'Age', deepface_common_prefix_num_layers + 1)),
            'dfsuffixgender': (
                DeepFaceSuffixNode, (['dfprefixembed'], [], 'Gender', deepface_common_prefix_num_layers + 1)),
            'dfsuffixrace': (
                DeepFaceSuffixNode, (['dfprefixembed'], [], 'Race', deepface_common_prefix_num_layers + 1)),
            'transfer': (ComplexTransferNode, (inp_col_list,)),
            'overhead': OverheadNode(inp_col_list),
            'simple': (SimpleTransferNode, (inp_col_list,)),
        }

    def __getitem__(self, item):
        node_info = self.NodeMappings[item]
        if type(node_info) is tuple:
            self.NodeMappings[item] = node_info[0](*node_info[1])
        return self.NodeMappings[item]

    def __contains__(self, item):
        return item in self.NodeMappings


NodeMappings = NodeMap()
