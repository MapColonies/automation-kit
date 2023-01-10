from jsonschema import Draft7Validator, FormatChecker
from jsonschema.exceptions import ValidationError


def validate_json(data, schema, field_values):
    """
    This function validates JSON fields by given schema and values
    :param data: the selected json data to be validated : dict
    :param schema: the selected schema to validate with : dict
    :param field_values: the selected fields to validate their values : dict
    :return:
    is_validate : dict{valid: bool , message: str }
    """
    is_validate = {}
    try:
        # Validate the data against the schema
        Draft7Validator(schema, format_checker=FormatChecker()).validate(data)

        # Check that the specified fields have the correct values
        for field, value in field_values.items():
            if data.get(field) != value:
                is_validate["valid"] = False
                is_validate["message"] = f"Field '{field}' has value '{data.get(field)}', expected '{value}'"
                raise ValidationError(f"Field '{field}' has value '{data.get(field)}', expected '{value}'")
            return is_validate

        is_validate["valid"] = True
        is_validate["message"] = "JSON is valid"
        return is_validate
    except ValidationError as e:
        is_validate["valid"] = False
        is_validate["message"] = str(e)
        return is_validate
