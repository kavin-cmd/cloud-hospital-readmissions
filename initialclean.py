import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsglue import DynamicFrame


def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(
                ctx,
                field.dataType,
                new_path + field.name,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(
                ctx,
                schema.elementType,
                path,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split(".")[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set(
                    [
                        item.strip() if isinstance(item, str) else item
                        for item in distinct_
                    ]
                )
            elif isinstance(distinct_, str):
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif (
            isinstance(schema, IntegerType)
            or isinstance(schema, LongType)
            or isinstance(schema, DoubleType)
        ):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output


def drop_nulls(
    glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx
) -> DynamicFrame:
    nullColumns = _find_null_fields(
        frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame
    )
    return DropFields.apply(
        frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://ccproject1/data.csv"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node clean data
cleandata_node1697059023581 = DropFields.apply(
    frame=S3bucket_node1,
    paths=[
        "diabetesMed",
        "metformin-pioglitazone",
        "metformin-rosiglitazone",
        "glimepiride-pioglitazone",
        "glipizide-metformin",
        "glyburide-metformin",
        "insulin",
        "citoglipton",
        "examide",
        "tolazamide",
        "troglitazone",
        "miglitol",
        "acarbose",
        "rosiglitazone",
        "pioglitazone",
        "metformin",
        "repaglinide",
        "nateglinide",
        "chlorpropamide",
        "glimepiride",
        "acetohexamide",
        "glipizide",
        "glyburide",
        "tolbutamide",
    ],
    transformation_ctx="cleandata_node1697059023581",
)

# Script generated for node Drop Null Fields
DropNullFields_node1697059183287 = drop_nulls(
    glueContext,
    frame=cleandata_node1697059023581,
    nullStringSet={"", "null", "?"},
    nullIntegerSet={-1},
    transformation_ctx="DropNullFields_node1697059183287",
)

# Script generated for node target
target_node1697059237845 = glueContext.getSink(
    path="s3://ccproject1",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="target_node1697059237845",
)
target_node1697059237845.setCatalogInfo(
    catalogDatabase="patient_readmission", catalogTableName="clean_data1"
)
target_node1697059237845.setFormat("csv")
target_node1697059237845.writeFrame(DropNullFields_node1697059183287)
job.commit()
