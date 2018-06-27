import pandas as pd
import xml.etree.ElementTree as ET


class XML2DataFrame:

    def __init__(self, xml_data):
        self.root = ET.XML(xml_data)

    def parse_element(self, element):
        #results = {}
        if element.tag == 'SEM21':
            #results['SEM21']
            return self.parse_middle(element)
        if element.tag == 'SEM02':
            return self.parse_middle(element)
        if element.tag == 'SEM03':
            return self.parse_middle(element)

        for child in list(element):
            child_result = self.parse_element(child)
            if child_result is not None:
                return child_result
                #if tag not in results.keys():
                #    results[tag] = []
                #results[tag].append(child_result)

        return None

    def parse_middle(self, element):
        boards = []
        records = []
        for child in list(element):
            if child.tag == 'RECORDS':
                record = self.parse_record(child)
                if record is not None:
                    records.append(record)
            else:
                board = self.parse_middle(child)
                if board is not None:
                    boards.append(board)

        if len(records) > 0:
            boards.append(pd.DataFrame(records))

        if len(boards) > 0:
            result = pd.concat(boards)
            for key in element.keys():
                result[key] = element.attrib.get(key)
            return result

        return None

    @staticmethod
    def parse_record(record):
        result = {}
        for key in record.keys():
            result[key] = record.attrib.get(key)
        return result

    def process_data(self):
        boards = list(filter(lambda board: board is not None, [self.parse_element(child) for child in list(self.root)]))
        return pd.concat(boards) if len(boards) > 0 else None

    def process_prices(self):
        rows  = [self.parse_record(row) for row in filter(lambda node: node.tag == "row", list(self.root))]
        if len(rows) > 0:
            return pd.DataFrame(rows)
        else:
            return None