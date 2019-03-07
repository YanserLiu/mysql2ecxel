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
	NetDev      string = "网络"
	ServerDev   string = "服务器"
)

type Mysql2Excel struct {
	rows       *sql.Rows
	DirType    string
	DevType    string
	SheeftType string
	Para       string
	File       string
	Dir        string
	Location   string
}

func init() {
	var err error
	dsn := "cmdbreader:mysql123456@tcp(172.18.38.219:3306)/cmdb?charset=utf8"
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
	if err := createMain(); err != nil {
		return
	}
	fmt.Println("<<<<<<<<<<<<<<<<<所有设备全部盘点完成，可以交差了！！！>>>>>>>>>>>>>>>>>>>")
	fmt.Println("5秒后该页面自动关闭")
	time.Sleep(5 * time.Second)
}

func createMain() error {
	mysql2excel := &Mysql2Excel{}

	mysql2excel.creatRootDir()

	count := mysql2excel.getLocationCount()
	ch := make(chan bool, count)
	rows, err := mysql2excel.getLocationRowsBySql()
	if err != nil {
		fmt.Println(err)
		return err
	}
	for rows.Next() {

		err := rows.Scan(&Para)
		if err != nil {
			fmt.Println(err)
		}
		mysql := &Mysql2Excel{Location: Para, Dir: mysql2excel.Dir}
		go mysql.creatExcelByLocation(ch)

	}
	for i := 0; i < count; i++ {
		<-ch
	}
	return nil
}

func (mysql2excel *Mysql2Excel) creatRootDir() {
	data := time.Now().Format("20060102")
	hour := time.Now().Hour()
	minute := time.Now().Minute()

	var min string
	if minute < 10 {
		min = fmt.Sprintf("0%d", minute)
	} else {
		min = fmt.Sprintf("%d", minute)
	}
	mysql2excel.Dir = fmt.Sprintf("%s%d%s", data, hour, min)

	_ = os.Mkdir(mysql2excel.Dir, os.ModePerm)
	// if err != nil {
	// 	fmt.Println(err)
	// }
}
func (mysql2excel *Mysql2Excel) creatExcelByLocation(ch chan bool) {
	//fmt.Println("<<<<<<<<<<<<<<<<<<<现在开始模块盘点，请稍等片刻>>>>>>>>>>>>>>>>>>>>>>>")
	// //库房
	//fmt.Println("创建目录", mysql2excel.Dir+"/"+mysql2excel.Location)
	err := os.Mkdir(mysql2excel.Dir+"/"+mysql2excel.Location, os.ModePerm)
	if err != nil {
		fmt.Println(err)
	}
	err = mysql2excel.createMain(RackNotNull)
	if err != nil {
		panic(err)
	}

	//模块

	err = mysql2excel.createStoreMain()
	if err != nil {
		panic(err)
	}
	ch <- true

}

func (mysql2excel *Mysql2Excel) createMain(DirType string) error {
	mysql2excel.DirType = DirType

	count := mysql2excel.getModulesCountByLocation()
	//fmt.Println(mysql2excel.Location, "机房下有模块个数", count)
	ch := make(chan bool, count)
	rows, err := mysql2excel.getModulesRowsByLocation()
	if err != nil {
		fmt.Println(err)
		return err
	}
	for rows.Next() {

		err := rows.Scan(&Para)
		if err != nil {
			fmt.Println(err)
		}
		mysql := &Mysql2Excel{Para: Para, Location: mysql2excel.Location, DirType: DirType, Dir: mysql2excel.Dir}
		go mysql.creatExcel(ch)

	}
	for i := 0; i < count; i++ {
		<-ch
	}
	return nil
}

func (mysql2excel *Mysql2Excel) createStoreMain() error {
	ch := make(chan bool, 1)
	mysql := &Mysql2Excel{Para: mysql2excel.Location, Location: mysql2excel.Location, DirType: RackIsNull, Dir: mysql2excel.Dir}
	go mysql.creatExcel(ch)

	<-ch

	return nil
}

func (mysql2excel *Mysql2Excel) getLocationCount() int {
	var sql, number string

	sql = "select count(*) as num from (select Location from Rack where deletedAt is NULL and RackModule is not NULL and Location is not NULL GROUP BY Location) c"

	err := db.QueryRow(sql).Scan(&number)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	count, _ := strconv.Atoi(number)
	return count
}

func (mysql2excel *Mysql2Excel) getModulesCountByLocation() int {
	var sql, number string
	sql = "select count(*) as num from (select RackModule from Rack where deletedAt is NULL and RackModule is not NULL and Location='" + mysql2excel.Location + "'  GROUP BY RackModule)c"
	err := db.QueryRow(sql).Scan(&number)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	count, _ := strconv.Atoi(number)
	return count
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

func (mysql2excel *Mysql2Excel) getLocationRowsBySql() (*sql.Rows, error) {
	sql := "select Location from Rack where deletedAt is NULL and RackModule is not NULL and Location is not NULL GROUP BY Location asc"
	//fmt.Println(sql)
	rows, err := db.Query(sql)
	if err != nil {
		panic(err)
		return nil, err
	}
	return rows, nil

}

func (mysql2excel *Mysql2Excel) getModulesRowsByLocation() (*sql.Rows, error) {

	sql := "select RackModule as Para from Rack where deletedAt is NULL and RackModule is not NULL and Location='" + mysql2excel.Location + "'  GROUP BY Para"
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
			return "select (@i:=@i+1) as 序号,c.* from (select NULL as 资产正常,NULL as 盘亏,SN as 序列号,Rack as 机架,startU as 起始U位,ManufactureType as 厂商型号,Manufacture as 厂商,OperationalStatus as 运行状态,IDC as 逻辑机房, Location as 位置, NULL as 备注 from Switch where deletedAt is NULL and Rack in (select RackName from Rack where deletedAt is NULL and Location='" + mysql2excel.Location + "' and RackModule='" + mysql2excel.Para + "')) c ,(select @i:=0) as it ORDER BY 机架,起始U位 asc;"
		}
		//模块 服务器 设备
		return "select (@i:=@i+1) as 序号,c.* from (select NULL as 资产正常,NULL as 盘亏,AssetNo as 资产编号,SN as 序列号,Rack as 机架,Placement as 机位编号,ManufactureType as 厂商型号,Manufacture as 厂商,MachineType,OperationalStatus as 运行状态, Location as 位置,IDC as 逻辑机房, NULL as 备注 from Server where deletedAt is NULL and  Rack in (select RackName from Rack where deletedAt is NULL and Location='" + mysql2excel.Location + "' and RackModule='" + mysql2excel.Para + "')) c ,(select @i:=0) as it ORDER BY 机架,机位编号 asc;"

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
				"(select RackName as 机架名, RackType as 业务类型,Location as 位置,IDC as 逻辑机房,RackStatus as 机架状态 from Rack where RackModule = '" + mysql2excel.Para + "' and Location= '" + mysql2excel.Location +
				"' and deletedAt is NULL ORDER BY 机架名 ASC )a left join (select Rack,Location,id as SwitchId from Switch where DeletedAt is NULL )b" +
				" on a.机架名=b.Rack and a.位置=b.Location where SwitchId is not NULL GROUP BY 机架名 HAVING count(1)>0 union all " +
				"select 0 as CMDB登记设备数量,NULL as 实际设备数量,机架名 from (select RackName as 机架名, RackType as 业务类型,Location as 位置,IDC as 逻辑机房,RackStatus as 机架状态 from Rack where RackModule = '" + mysql2excel.Para + "' and Location= '" + mysql2excel.Location + "' and deletedAt is NULL ORDER BY 机架名 ASC " +
				")a left join (select Rack,Location,id as SwitchId from Switch where DeletedAt is NULL )b on a.机架名=b.Rack and a.位置=b.Location where SwitchId is NULL GROUP BY 机架名 HAVING count(1)>0) c left join " +
				"(select RackType as 业务类型, Location as 位置, IDC as 逻辑机房, RackStatus as 机架状态,RackName from Rack where DeletedAt is NULL )d on c.机架名=d.RackName ORDER BY 机架名 asc"
		} else {
			return "select CMDB登记设备数量,实际设备数量,机架名, 业务类型,位置,逻辑机房,机架状态 from (select count(*) as CMDB登记设备数量,NULL as 实际设备数量,机架名 from " +
				"(select RackName as 机架名, RackType as 业务类型,Location as 位置,IDC as 逻辑机房,RackStatus as 机架状态 from Rack where RackModule = '" + mysql2excel.Para + "' and Location= '" + mysql2excel.Location +
				"' and deletedAt is NULL ORDER BY 机架名 ASC )a left join (select Rack,Location,id as ServerId from Server where DeletedAt is NULL )b" +
				" on a.机架名=b.Rack and a.位置=b.Location where ServerId is not NULL GROUP BY 机架名 HAVING count(1)>0 union all " +
				"select 0 as CMDB登记设备数量,NULL as 实际设备数量,机架名 from (select RackName as 机架名, RackType as 业务类型,Location as 位置,IDC as 逻辑机房,RackStatus as 机架状态 from Rack where RackModule = '" + mysql2excel.Para + "' and Location= '" + mysql2excel.Location + "' and deletedAt is NULL ORDER BY 机架名 ASC " +
				")a left join (select Rack,Location,id as ServerId from Server where DeletedAt is NULL )b on a.机架名=b.Rack and a.位置=b.Location where ServerId is NULL GROUP BY 机架名 HAVING count(1)>0) c left join " +
				"(select RackType as 业务类型, Location as 位置, IDC as 逻辑机房, RackStatus as 机架状态,RackName from Rack where DeletedAt is NULL )d on c.机架名=d.RackName ORDER BY 机架名 asc"

		}
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
	var name, fileName string
	if mysql2excel.DirType == RackIsNull {

		fileName = mysql2excel.Dir + "/" + mysql2excel.Location + "/" + "库房-" + mysql2excel.DevType + "设备.xlsx"
	} else {
		name = strings.Replace(mysql2excel.Para, ":", "_", -1)
		fileName = mysql2excel.Dir + "/" + mysql2excel.Location + "/" + "模块-" + name + "-" + mysql2excel.DevType + "设备.xlsx"
	}
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
