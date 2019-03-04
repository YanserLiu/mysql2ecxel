package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tealeg/xlsx"
)

var db *sql.DB
var RackModule string
var Location string
var Para string

type Info struct {
	DirType    string
	DevType    string
	SheeftType string
	Para       string
}

const (
	Sheeft1     string = "CMDB设备导出表"
	Sheeft2     string = "机柜设备数量表"
	Sheeft3     string = "盘盈记录表"
	RackIsNull  string = "库房"
	RackNotNull string = "模块"
	NetDev      string = "交换机"
	ServerDev   string = "服务器"
)

type Mysql2Excel struct {
	rows       *sql.Rows
	DirType    string
	DevType    string
	SheeftType string
	Para       string
	File       string
}

func init() {
	var err error
	dsn := "****:****@tcp(****:3306)/****?charset=utf8"
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}

}

func main() {
	fmt.Println("<<<<<<<<<<<<<<<<<<<现在开始模块盘点，请稍等片刻>>>>>>>>>>>>>>>>>>>>>>>")
	//库房
	err := createMain(RackNotNull)
	if err != nil {
		panic(err)
	}

	//模块

	fmt.Println("<<<<<<<<<<<<<<<<<<<现在开始库房盘点，请稍等片刻>>>>>>>>>>>>>>>>>>>>>>>")
	err := createMain(RackIsNull)
	if err != nil {
		panic(err)
	}

	fmt.Println("<<<<<<<<<<<<<<<<<所有设备全部盘点完成，可以交差了！！！>>>>>>>>>>>>>>>>>>>")
	fmt.Println("5秒后该页面自动关闭")
	time.Sleep(5 * time.Second)
	return

}
func createMain(DirType string) error {
	mysql2excel := &Mysql2Excel{DirType: DirType}
	err := os.Mkdir(DirType, os.ModePerm)
	if err != nil {
		fmt.Println(err)
	}
	count := mysql2excel.getCount()
	fmt.Println(DirType, "统计个数:", count)
	ch := make(chan bool, count)
	rows, err := mysql2excel.getTypeRowsBySql()
	if err != nil {
		fmt.Println(err)
		return err
	}
	for rows.Next() {
		mysql := &Mysql2Excel{DirType: DirType}
		err := rows.Scan(&Para)
		if err != nil {
			fmt.Println(err)
		}
		mysql.Para = Para
		go mysql.creatExcel(ch)

	}
	for i := 0; i < count; i++ {
		<-ch
	}
	return nil
}

func (mysql2excel *Mysql2Excel) getCount() int {
	var sql, number string
	if mysql2excel.DirType == RackIsNull {
		sql = "select count(*) as num from (select * from (select Location from Switch where deletedAt is NULL and Rack is NULL GROUP BY Location UNION all select Location from Server where deletedAt is NULL and Rack is NULL GROUP BY Location) c group by Location) c"
	} else {
		sql = "select count(*) as num from (select RackModule from Rack where deletedAt is NULL and RackModule is not NULL  GROUP BY RackModule) c"
	}
	err := db.QueryRow(sql).Scan(&number)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	count, _ := strconv.Atoi(number)
	return count
}

func (mysql2excel *Mysql2Excel) getTypeRowsBySql() (*sql.Rows, error) {
	if mysql2excel.DirType == RackIsNull {
		sql := "select * from (select Location as Para from Switch where deletedAt is NULL and Rack is NULL GROUP BY Location UNION all select Location as Para from Server where deletedAt is NULL and Rack is NULL GROUP BY Location) c group by Para"
		//fmt.Println(sql)
		rows, err := db.Query(sql)
		if err != nil {
			panic(err)
			return nil, err
		}
		return rows, nil
	}
	sql := "select RackModule as Para from Rack where deletedAt is NULL and RackModule is not NULL  GROUP BY Para"
	//fmt.Println(sql)
	rows, err := db.Query(sql)
	if err != nil {
		panic(err)
		return nil, err
	}
	return rows, nil

}

func (mysql2excel *Mysql2Excel) getSheeft1Sql() string {
	//fmt.Println("CMDB导出表")
	if mysql2excel.DirType == RackIsNull { //库房模式
		if mysql2excel.DevType == NetDev { //库房 网络 设备
			return "select (@i:=@i+1) as 序号,c.* from (select NULL as 资产正常,NULL as 盘亏,SN as 序列号,IFNULL(Rack,'库房') as 机架,startU as 起始U位,ManufactureType as 厂商型号,Manufacture as 厂商,OperationalStatus as 运行状态,IDC as 逻辑机房, Location as 位置, NULL as 备注 from Switch where deletedAt is NULL and Rack is NULL and Location='" + mysql2excel.Para + "') c ,(select @i:=0) as it;"
		}
		//库房 服务器 设备
		return "select (@i:=@i+1) as 序号,c.* from (select NULL as 资产正常,NULL as 盘亏,AssetNo as 资产编号,SN as 序列号,Rack as 机架,Placement as 机位编号,ManufactureType as 厂商型号,Manufacture as 厂商,MachineType,OperationalStatus as 运行状态, Location as 位置,IDC as 逻辑机房, NULL as 备注 from Server where deletedAt is NULL and  Rack is NULL and Location='" + mysql2excel.Para + "') c ,(select @i:=0) as it;"

	}
	if mysql2excel.DirType == RackNotNull { //模块模式
		if mysql2excel.DevType == NetDev { //模块 网络 设备
			return "select (@i:=@i+1) as 序号,c.* from (select NULL as 资产正常,NULL as 盘亏,SN as 序列号,Rack as 机架,startU as 起始U位,ManufactureType as 厂商型号,Manufacture as 厂商,OperationalStatus as 运行状态,IDC as 逻辑机房, Location as 位置, NULL as 备注 from Switch where deletedAt is NULL and Rack in (select RackName from Rack where deletedAt is NULL and RackModule='" + mysql2excel.Para + "')) c ,(select @i:=0) as it ORDER BY 机架,起始U位 asc;"
		}
		//模块 服务器 设备
		return "select (@i:=@i+1) as 序号,c.* from (select NULL as 资产正常,NULL as 盘亏,AssetNo as 资产编号,SN as 序列号,Rack as 机架,Placement as 机位编号,ManufactureType as 厂商型号,Manufacture as 厂商,MachineType,OperationalStatus as 运行状态, Location as 位置,IDC as 逻辑机房, NULL as 备注 from Server where deletedAt is NULL and  Rack in (select RackName from Rack where deletedAt is NULL and RackModule='" + mysql2excel.Para + "')) c ,(select @i:=0) as it ORDER BY 机架,机位编号 asc;"

	}
	return ""
}
func (mysql2excel *Mysql2Excel) getSheeft2Sql() string {
	if mysql2excel.DirType == RackIsNull {
		if mysql2excel.DevType == NetDev {
			return "select NULL as 序号,NULL as 序列号,NULL as 资产编号, NULL as 运行状态,NULL as 厂商型号,NULL as 厂商,NULL as 逻辑机房,NULL as 位置,NULL as 机架,NULL as 机架编号,NULL as 备注 from IDC"
		} else {
			return "select NULL as 序号,NULL as 序列号,NULL as 资产编号, NULL as 运行状态,NULL as 厂商型号,NULL as 厂商,NULL as 逻辑机房,NULL as 位置,NULL as 机架,NULL as 机架编号,NULL as 备注 from IDC"
		}

	}
	if mysql2excel.DirType == RackNotNull {
		if mysql2excel.DevType == NetDev {
			return "select CMDB登记设备数量,实际设备数量,机架名, 业务类型,位置,逻辑机房,机架状态 from (select count(*) as CMDB登记设备数量,NULL as 实际设备数量,机架名 from " +
				"(select RackName as 机架名, RackType as 业务类型,Location as 位置,IDC as 逻辑机房,RackStatus as 机架状态 from Rack where RackModule = '" + mysql2excel.Para +
				"' and deletedAt is NULL ORDER BY 机架名 ASC )a left join (select Rack,Location,id as SwitchId from Switch where DeletedAt is NULL )b" +
				" on a.机架名=b.Rack and a.位置=b.Location where SwitchId is not NULL GROUP BY 机架名 HAVING count(1)>0 union all " +
				"select 0 as CMDB登记设备数量,NULL as 实际设备数量,机架名 from (select RackName as 机架名, RackType as 业务类型,Location as 位置,IDC as 逻辑机房,RackStatus as 机架状态 from Rack where RackModule = '" + mysql2excel.Para + "' and deletedAt is NULL ORDER BY 机架名 ASC " +
				")a left join (select Rack,Location,id as SwitchId from Switch where DeletedAt is NULL )b on a.机架名=b.Rack and a.位置=b.Location where SwitchId is NULL GROUP BY 机架名 HAVING count(1)>0) c left join " +
				"(select RackType as 业务类型, Location as 位置, IDC as 逻辑机房, RackStatus as 机架状态,RackName from Rack where DeletedAt is NULL )d on c.机架名=d.RackName ORDER BY 机架名 asc"
		} else {
			return "select CMDB登记设备数量,实际设备数量,机架名, 业务类型,位置,逻辑机房,机架状态 from (select count(*) as CMDB登记设备数量,NULL as 实际设备数量,机架名 from " +
				"(select RackName as 机架名, RackType as 业务类型,Location as 位置,IDC as 逻辑机房,RackStatus as 机架状态 from Rack where RackModule = '" + mysql2excel.Para +
				"' and deletedAt is NULL ORDER BY 机架名 ASC )a left join (select Rack,Location,id as ServerId from Server where DeletedAt is NULL )b" +
				" on a.机架名=b.Rack and a.位置=b.Location where ServerId is not NULL GROUP BY 机架名 HAVING count(1)>0 union all " +
				"select 0 as CMDB登记设备数量,NULL as 实际设备数量,机架名 from (select RackName as 机架名, RackType as 业务类型,Location as 位置,IDC as 逻辑机房,RackStatus as 机架状态 from Rack where RackModule = '" + mysql2excel.Para + "' and deletedAt is NULL ORDER BY 机架名 ASC " +
				")a left join (select Rack,Location,id as ServerId from Server where DeletedAt is NULL )b on a.机架名=b.Rack and a.位置=b.Location where ServerId is NULL GROUP BY 机架名 HAVING count(1)>0) c left join " +
				"(select RackType as 业务类型, Location as 位置, IDC as 逻辑机房, RackStatus as 机架状态,RackName from Rack where DeletedAt is NULL )d on c.机架名=d.RackName ORDER BY 机架名 asc"

		}
		// if mysql2excel.DevType == NetDev {
		// 	return "select CMDB登记设备数量,NULL as 实际设备数量,机架名, 业务类型,位置,逻辑机房,机架状态 from (select Rack as 机架名,count(*) as CMDB登记设备数量 from Switch where deletedAt is NULL and Rack in (select RackName from  Rack where deletedAt is NULL and RackModule='" + mysql2excel.Para + "') GROUP BY 机架名 HAVING count(1)>0)a left join (select RackName,RackType as 业务类型, Location as 位置, IDC as 逻辑机房, RackStatus as 机架状态 from Rack ) b on a.机架名 =b.RackName"
		// } else {
		// 	return "select CMDB登记设备数量,NULL as 实际设备数量,机架名, 业务类型,位置,逻辑机房,机架状态 from (select Rack as 机架名,count(*) as CMDB登记设备数量 from Server where deletedAt is NULL and Rack in (select RackName from  Rack where deletedAt is NULL and RackModule='" + mysql2excel.Para + "') GROUP BY 机架名 HAVING count(1)>0)a left join (select RackName,RackType as 业务类型, Location as 位置, IDC as 逻辑机房, RackStatus as 机架状态 from Rack ) b on a.机架名 =b.RackName"
		// }
	}
	return ""
}
func (mysql2excel *Mysql2Excel) getSheeft3Sql() string {
	if mysql2excel.DevType == NetDev { //网络设备
		return "select NULL as 序号,NULL as 序列号,NULL as 机架,NULL as 起始U位,NULL as 厂商型号,NULL as 厂商,NULL as 运行状态,NULL as 逻辑机房,NULL as 位置,NULL as 备注 from IDC"
	}

	//服务器设备
	return "select NULL as 序号,NULL as 资产编号,NULL as 序列号,NULL as 机架,NULL as 机架编号,NULL as 厂商型号,NULL as 厂商,NULL as MachineType,NULL as 运行状态,NULL as 位置,NULL as 逻辑机房,NULL as 备注 from IDC"

}

func (mysql2excel Mysql2Excel) saveSheeftByRows(file *xlsx.File, sql, sheeftName string) error {
	rows, err := getRowsBySql(sql)
	if err != nil {
		return err
	}
	sheet, err := file.AddSheet(sheeftName)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	//写column数据
	columnRow := sheet.AddRow()
	columnLen := len(columns)
	for _, name := range columns {
		cell := columnRow.AddCell()
		cell.Value = name
	}

	scanArgs := make([]interface{}, columnLen)
	values := make([][]byte, columnLen)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		rows.Scan(scanArgs...)
		row := sheet.AddRow()
		for _, v := range values {
			cell := row.AddCell()
			cell.Value = string(v)
		}
	}
	return nil
}

func (mysql2excel *Mysql2Excel) creatExcel(ch chan bool) {

	err := mysql2excel.createExcelByDevType(NetDev)
	if err != nil {
		return
	}

	err = mysql2excel.createExcelByDevType(ServerDev)
	if err != nil {
		return
	}

	ch <- true
}

func (mysql2excel *Mysql2Excel) createExcelByDevType(dev string) error {
	mysql2excel.DevType = dev
	if err := mysql2excel.createExcelFile(); err != nil {
		return err
	}

	return nil
}

func (mysql2excel *Mysql2Excel) createExcelFile() error {
	var err error
	name := strings.Replace(mysql2excel.Para, ":", "_", -1)
	fileName := mysql2excel.DirType + "/" + name + mysql2excel.DevType + "设备.xlsx"
	mysql2excel.File, err = filepath.Abs(fileName)
	if err != nil {
		fmt.Println(err)
		return err
	}
	file := xlsx.NewFile()
	err = mysql2excel.saveSheeftToExcel(file)
	if err != nil {
		return err
	}
	err = file.Save(mysql2excel.File)
	if err != nil {
		return err
	}
	fmt.Println(fileName, "盘点完成")
	return nil
}

func (mysql2excel *Mysql2Excel) saveSheeftToExcel(file *xlsx.File) error {
	//mysql2excel.SheeftType = Sheeft1
	sheeft1Sql := mysql2excel.getSheeft1Sql()
	err := mysql2excel.creteSheet(sheeft1Sql, Sheeft1, file)
	if err != nil {
		return err
	}

	sheeft2Sql := mysql2excel.getSheeft2Sql()
	err = mysql2excel.creteSheet(sheeft2Sql, Sheeft2, file)

	if err != nil {
		fmt.Println(err)
		return err
	}

	sheeft3Sq1 := mysql2excel.getSheeft3Sql()
	err = mysql2excel.creteSheet(sheeft3Sq1, Sheeft3, file)
	if err != nil {
		return err
	}
	return nil
}

func (mysql2excel *Mysql2Excel) creteSheet(sql, SheeftType string, file *xlsx.File) error {
	//fmt.Println(sql)
	rows, err := getRowsBySql(sql)
	if err != nil {
		return err
	}

	sheet, err := file.AddSheet(SheeftType)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	//写column数据
	columnRow := sheet.AddRow()
	columnLen := len(columns)
	for _, name := range columns {
		cell := columnRow.AddCell()
		cell.Value = name
	}

	scanArgs := make([]interface{}, columnLen)
	values := make([][]byte, columnLen)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		rows.Scan(scanArgs...)
		row := sheet.AddRow()
		for _, v := range values {
			cell := row.AddCell()
			cell.Value = string(v)
		}
	}
	return nil
}

func getRowsBySql(sqlStr string) (*sql.Rows, error) {
	//查询获取结果
	stmt, err := db.Prepare(sqlStr)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return rows, nil
}
