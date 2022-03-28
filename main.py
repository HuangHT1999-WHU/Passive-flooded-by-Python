# 处理日期和时间的标准库
import time,datetime
# 用于查找符合特定规则的文件路径名
import glob
# 基于 NumPy 的数据操作库，提供了高效地操作大型数据集所需的工具和方法
import pandas as pd

# 读取文件夹中的csv文件
def extract_track_files(path):
    # 使用glob.glob()获取指定目录(path)下所有的csv文件，
    files = [filename for filename in glob.glob(path + '*.csv')]
    return files

""" ***代码正文部分*** """
# 记录开始执行代码的时间
time_maincode_start = time.localtime()
print("主体代码开始执行的时间为：%s\n" % time.strftime("%Y-%m-%d %H:%M:%S", time_maincode_start))

# 读取数据文件
# Precipitation_depth_data_folder_path为已排水径流深的分区统计表格所在的目录相对路径
Precipitation_depth_data_folder_path = 'data/Precipitation_depth/0000/'
# 读取已排水径流深文件夹中的csv文件路径列表，注意，此处文件名的顺序为（例）：0719-0,1,10,11，12，... ,19，2,20,21,22,23,3,4，...，9
Precipitation_depth_files = extract_track_files(Precipitation_depth_data_folder_path)
# 读取DEM的子流域区域统计的csv文件
DEM_file = pd.read_csv("data/DEM/DEM子流域区域统计.csv")

# DEM中单个栅格的面积为：12.5*12.5=156.25平方米，DEM子流域区域统计中的统计项为面积（单位：平方米），在处理前需要将其转换为平方毫米
DEM_area_conversion_parameter = 1000000
# 已排水径流深中单个栅格的面积为：10*10=100平方米=100000000平方毫米
Area_Precipitation_depth = 100000000

# 循环读取各个时段的已排水径流深进行计算
for Precipitation_depth_file_index in range(len(Precipitation_depth_files)):
    print("现在开始读取处理 %s 的数据文件" % Precipitation_depth_files[Precipitation_depth_file_index][-15:-8])
    # 读取已排水径流深的分区统计表格文件
    Precipitation_depth_file = pd.read_csv(Precipitation_depth_files[Precipitation_depth_file_index])
    # 只保留分区统计表格中的FID和SUM统计数据
    Precipitation_depth_total = Precipitation_depth_file[['FID', 'SUM']]
    # 将单个栅格的面积纳入考虑，得到每一个子流域的总积水量（单位：立方毫米），并将dataframe重命名
    Precipitation_depth_total.loc[:, 'SUM'] *= Area_Precipitation_depth
    Precipitation_volume = Precipitation_depth_total.rename(columns={'SUM': 'Volume'})

    # 定义一个dataframe，用来保存每一个时段的积水情况，每一个时段结束循环后输出
    DEM_output_df = pd.DataFrame()
    # 循环读取子流域的高程值，计算从最低点开始积水的情况下子流域内各类高程的积水情况
    for FID in range(len(DEM_file)):
        # 读取DEM子流域区域统计文件中的第FID行，并生成DataFrame，字段名分别为：高程height和栅格面积area
        dict_DEM_file_row = {'height': DEM_file.iloc[FID].index, 'area': DEM_file.iloc[FID].values}
        DEM_df = pd.DataFrame(dict_DEM_file_row)
        # 去除掉首行表示FID的记录
        DEM_df = DEM_df.drop([0])
        # 通过取反，来表示不需要数量 area 为0的记录
        DEM_without0_df = DEM_df[~DEM_df['area'].isin([0])].reset_index(drop=True)
        # 新建一列，字段名设置为 FID，表示当前正在处理的子流域编码
        DEM_without0_df.insert(0, 'FID', FID)
        # 新建一列，字段名设置为 Additional_water，表示在当前栅格上积留的水量
        DEM_without0_df.insert(len(DEM_without0_df.columns), 'Additional_water', 0)
        # 设置一个参数 Average_ponding_depth，表示当前子流域的平均渍水深度（单位：米）
        Average_ponding_depth = 0

        # 如果当前的子流域内没有积水需要分配，则跳出循环
        if Precipitation_volume[Precipitation_volume['FID'] == FID]['Volume'].values[0] == 0:
            continue

        # 如果有积水需要分配则进一步计算
        # 当前子流域在这一时段的降水积水总量
        Precipitation_volume_currentFID = Precipitation_volume[Precipitation_volume['FID'] == FID]['Volume'].values[0]

        if Precipitation_volume_currentFID > 0:
            # 利用循环逐行检查子流域中的最低点
            # 积水计算的增量值设置为0.1毫米
            depth_delta = 0.1
            # 设置一个参数 area_computed_total，表示当前已经计算积水的栅格面积
            area_computed_total = 0
            # 设置一个参数 water_used_total，表示当前已经使用的总积水体积（单位：立方毫米）
            water_used_total = 0
            # 使用iterrows()迭代器对Dataframe进行遍历，返回两个元组：索引列index、行数据row（需要注意此行的每一个字段在row中变成了行，即做了个转置）
            for index, row in DEM_without0_df.iterrows():
                # 当已经使用的水量超过降水的积水总量时，跳出循环，结束当前子流域的计算
                if water_used_total >= Precipitation_volume_currentFID:
                    # 计算当前子流域的平均渍水深度，如果参与积水的栅格面积不超过五万平方米，则以五万平方米参与计算平均深度
                    if area_computed_total <= 50000:
                        area_computed_total_count = 50000
                    else:
                        area_computed_total_count = area_computed_total
                    # 计算当前子流域上的平均渍水深度
                    Average_ponding_depth = round((Precipitation_volume_currentFID / (area_computed_total_count * DEM_area_conversion_parameter)) * 0.001, 5)
                    break
                # 当遍历到子流域中的最后一类高程栅格时，使用的积水量尚未超过降水的积水总量时，将剩余的积水一并进行计算
                elif (index == (len(DEM_without0_df) - 1)) & (water_used_total < Precipitation_volume_currentFID):
                    # 将新加入计算的栅格个数累加到area_computed_total中
                    area_computed_total += row['area']
                    # 计算剩余需要的计算次数
                    for i in range(int((Precipitation_volume_currentFID - water_used_total) / (
                            depth_delta * area_computed_total * DEM_area_conversion_parameter) + 1)):
                        # 循环增加积水的使用量，直到用完总积水体积为止
                        water_used_total += area_computed_total * depth_delta * DEM_area_conversion_parameter
                        DEM_without0_df.loc[0:index, 'Additional_water'] += depth_delta
                else:
                    # 将新加入计算的栅格个数累加到area_computed_total中
                    area_computed_total += row['area']
                    # 由于栅格的高程差精度为1m，即1000毫米，从而计算循环次数
                    for i in range(int(1000 / depth_delta)):
                        # 循环增加积水的使用量，直到用完总积水体积为止
                        water_used_total += area_computed_total * depth_delta * DEM_area_conversion_parameter
                        DEM_without0_df.loc[0:index, 'Additional_water'] += depth_delta
                        if water_used_total > Precipitation_volume_currentFID:
                            break

        # 将栅格上积留的水量为零的行去除
        DEM_without0_df = DEM_without0_df.loc[~DEM_without0_df['Additional_water'].isin([0])].reset_index(drop=True)
        # 新建一列，字段名设置为 Ponding_level，表示在当前栅格上的积水水位线海拔高度，单位：米
        DEM_without0_df.insert(len(DEM_without0_df.columns), 'Ponding_level', 0.0)
        # 新建一列，字段名设置为 Average_ponding_depth，表示在当前子流域上的平均渍水深度，单位：米
        DEM_without0_df.insert(len(DEM_without0_df.columns), 'Average_ponding_depth', 0)
        # 计算积水水位线海拔高度，公式为 Ponding_level = height + 0.001 * Additional_water
        for index3, row3 in DEM_without0_df.iterrows():
            # 由于0.001相乘会产生一个无穷小的尾数，因此使用round()用来进行四舍五入，否则在进行去重时会无法判定Ponding_level重复
            DEM_without0_df.loc[index3, 'Ponding_level'] += round((float(row3['height']) + 0.001 * float(row3['Additional_water'])), 3)
            # 写入平均渍水深度
            DEM_without0_df.loc[index3, 'Average_ponding_depth'] += Average_ponding_depth
        # 根据FID和Ponding_level两列来去除重复项
        DEM_without0_df_final = DEM_without0_df.drop_duplicates(subset=['FID', 'Ponding_level'])
        DEM_output_df = pd.concat([DEM_output_df, DEM_without0_df_final])
        '''单个时段的各个子流域积水分配量计算到此为止'''
    # 设置输出的单个时段的各个子流域积水分配量路径和文件名
    output_path = 'data/output_files/0000/子流域积水分配量' + Precipitation_depth_files[Precipitation_depth_file_index][-15:-8]+'.csv'
    DEM_output_df.to_csv(output_path, index=False)

# 记录结束执行代码的时间
time_maincode_end = time.localtime()
print("主体代码结束执行的时间为：%s" % time.strftime("%Y-%m-%d %H:%M:%S", time_maincode_end))
time_maincode_start = datetime.datetime.strptime(time.strftime("%Y-%m-%d %H:%M:%S", time_maincode_start),"%Y-%m-%d %H:%M:%S")
time_maincode_end = datetime.datetime.strptime(time.strftime("%Y-%m-%d %H:%M:%S", time_maincode_end),"%Y-%m-%d %H:%M:%S")
# 结束时间减去开始时间得到运行的时间
maincode_seconds = (time_maincode_end - time_maincode_start).seconds
print("主体代码执行的耗时为：%d 秒" % maincode_seconds)





