package main
import(
	"fmt"
)
func rotateleft(s string, k int)string{
	a:=len(s)
	k=k%a  
	runes:=[]rune(s)
    result:=make([]rune,a)
	for i:=0;i<a;i++{
		newSty:=(i+a-k)%a
		result[newSty]=runes[i]
	}
	return string(result)
}
func main(){
	str:="abcdef"
	k:=2
	rotated:=rotateLeft(str,k)
	fmt.Println("Original string:",str)
	fmt.Println("Rotated string:",rotated)
}