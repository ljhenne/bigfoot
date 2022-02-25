from enum import Enum
from typing import Any, List, Optional

from google.cloud.bigquery.schema import SchemaField
from pydantic import BaseModel, Field

from bigfoot.constants import SHEETS_HEADERS
from bigfoot.bigfoot_schema import BigfootSchema


class Dimension(str, Enum):
    ROWS = "ROWS"


class DimensionRange(BaseModel):
    sheet_id: Optional[int]
    dimension: Dimension
    start_index: int
    end_index: int


class DimensionGroup(BaseModel):
    range: DimensionRange


class ExtendedValue(BaseModel):
    number_value: Optional[float]
    string_value: Optional[str]
    bool_value: Optional[bool]

    # Unimplemented
    # formula_value: str
    # error_value: ErrorValue


class TextFormat(BaseModel):
    bold: bool


class CellFormat(BaseModel):
    text_format: TextFormat


class CellData(BaseModel):
    user_entered_value: ExtendedValue
    user_entered_format: Optional[CellFormat]


class RowData(BaseModel):
    values: List[CellData]

    def append(self, value: CellData) -> None:
        self.values.append(value)

    @classmethod
    def from_list(cls, values: List[Any], bolded: bool = False) -> 'RowData':
        cell_data = list()
        for value in values:
            if isinstance(value, str) or value is None:
                extended_value = ExtendedValue(string_value=value)
            elif isinstance(value, int):
                extended_value = ExtendedValue(number_value=value)
            elif isinstance(value, tuple) and not value:
                extended_value = ExtendedValue(string_value=None)
            else:
                raise ValueError(f"Unsupported value type: {type(value)}; value: {value}")
            if bolded:
                user_entered_format = CellFormat(text_format=TextFormat(bold=True))
                cell_data.append(CellData(user_entered_value=extended_value, user_entered_format=user_entered_format))
            else:
                cell_data.append(CellData(user_entered_value=extended_value))
        return cls(values=cell_data)


class GridData(BaseModel):
    start_row: int
    start_column: int
    row_data: List[RowData]

    # Unimplemented
    # row_metadata: List[DimensionProperties]
    # column_metadata: List[DimensionProperties]

    @classmethod
    def headers_grid_data(cls) -> 'GridData':
        row_data = RowData.from_list(values=SHEETS_HEADERS, bolded=True)
        return cls(start_row=0, start_column=0, row_data=[row_data])


class Properties(BaseModel):
    title: str
    sheet_id: Optional[str] = Field(None, alias="sheetId")


class AddDimensionGroupRequest(BaseModel):
    add_dimension_group: DimensionGroup


class BatchUpdateRequestBody(BaseModel):
    requests: List[AddDimensionGroupRequest]

    @classmethod
    def add_dimension_groups_request(cls, dimension_groups: List[DimensionGroup]) -> 'BatchUpdateRequestBody':
        requests = list()
        for dimension_group in dimension_groups:
            requests.append(AddDimensionGroupRequest(add_dimension_group=dimension_group))
        return cls(requests=requests)


class Sheet(BaseModel):
    data: Optional[List[GridData]]
    properties: Properties


class GoogleSpreadsheet(BaseModel):
    # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#Spreadsheet
    properties: Properties
    sheets: List[Sheet]


class CreateSheetResponse(BaseModel):
    sheets: List[Sheet]
    spreadsheet_id: str = Field(alias="spreadsheetId")


class GoogleSheetsSchema(BigfootSchema):

    def __init__(self, name: str):
        super().__init__(name)

    def mk_dimension_groups(self, sheet_id) -> List[DimensionGroup]:
        dimension_groups: List[DimensionGroup] = list()

        def _parse(fields: List[SchemaField], i: int = 0) -> int:
            start = i
            for field in fields:
                i += 1
                if field.field_type == "RECORD" and field.fields:
                    field_count = _parse(field.fields, i)
                    range_start = i + 1
                    dimension_range = DimensionRange(
                        sheet_id=sheet_id,
                        dimension=Dimension.ROWS,
                        start_index=range_start,
                        end_index=range_start + field_count
                    )
                    i += field_count
                    dimension_group = DimensionGroup(range=dimension_range)
                    dimension_groups.append(dimension_group)
            return i - start
        _parse(self.api_repr)
        return dimension_groups

    def mk_google_spreadsheet(self) -> GoogleSpreadsheet:
        properties = Properties(title=self.name)
        rows: List[RowData] = list()

        def _parse_fields(fields: List[SchemaField], prefix: List):
            for field in fields:
                prefix.append(field.name)
                row_values = [
                    ".".join(prefix + [field.name]),
                    field.name,
                    field.field_type,
                    field.mode,
                    field.description,
                    field.policy_tags,
                    field.precision,
                    field.scale,
                    field.max_length
                ]
                rows.append(RowData.from_list(row_values))
                if field.field_type == "RECORD" and field.fields:
                    _parse_fields(field.fields, prefix)
                prefix.pop()
        _parse_fields(self.api_repr, list())
        fields_grid_data = GridData(start_row=1, start_column=0, row_data=rows)
        headers_grid_data = GridData.headers_grid_data()
        sheet = Sheet(properties=properties, data=[headers_grid_data, fields_grid_data])
        return GoogleSpreadsheet(properties=properties, sheets=[sheet])
