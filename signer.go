package main

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func ExecutePipeline(jobs ...job) {
	chanLen := len(jobs) + 1
	channels := make([]chan interface{}, chanLen, chanLen)
	for i := 0; i < chanLen; i++ {
		channels[i] = make(chan interface{}, 100)
	}
	close(channels[0])
	wg := &sync.WaitGroup{}
	for i := range jobs {
		wg.Add(1)
		go func(job job, in, out chan interface{}) {
			defer close(out)
			defer wg.Done()
			job(in, out)
		}(jobs[i], channels[i], channels[i+1])
		runtime.Gosched()
	}
	wg.Wait()
}

// SingleHash считает значение crc32(data)+"~"+crc32(md5(data)) ( конкатенация двух строк через ~), где data - то что
// пришло на вход (по сути - числа из первой функции)

func SingleHash(in, out chan interface{}) {
	md5Mutex := &sync.Mutex{}

	for val := range in {
		intVal, ok := val.(int)
		if !ok {
			panic(fmt.Sprintf("value is not int '%s'", val))
		}
		strVal := strconv.Itoa(intVal)
		log("SingleHash start", strVal)

		md5Chan := async(func(c chan string) {
			defer md5Mutex.Unlock()
			md5Mutex.Lock()
			md5 := DataSignerMd5(strVal)
			c <- md5
			log("SingleHash md5", strVal, md5)
		})
		crc32Chan := async(func(c chan string) {
			crc32 := DataSignerCrc32(strVal)
			c <- crc32
			log("SingleHash crc32", strVal, crc32)
		})
		resultChan := async(func(c chan string) {
			md5 := <-md5Chan
			md5crc32 := DataSignerCrc32(md5)
			log("SingleHash md5crc32", strVal, md5crc32)
			crc32 := <-crc32Chan
			hash := crc32 + `~` + md5crc32
			c <- hash
			log("SingleHash done", strVal, hash)
		})

		out <- resultChan
		runtime.Gosched()
	}
}

// MultiHash считает значение crc32(th+data)) (конкатенация цифры, приведённой к строке и строки), где th=0..5
// ( т.е. 6 хешей на каждое входящее значение), потом берёт конкатенацию результатов в порядке расчета (0..5), где
// data - то что пришло на вход (и ушло на выход из SingleHash)

const MultiHashCount = 6

func MultiHash(in, out chan interface{}) {
	for val := range in {
		chanVal, ok := val.(chan string)
		if !ok {
			panic(fmt.Sprintf("value is not string channel '%s'", val))
		}
		resultChan := async(func(c chan string) {
			strVal := <-chanVal
			log("MultiHash start", strVal)
			results := [MultiHashCount]string{}
			wg := &sync.WaitGroup{}
			for i := 0; i < MultiHashCount; i++ {
				wg.Add(1)
				go func(i int, wg *sync.WaitGroup) {
					defer wg.Done()
					strI := strconv.Itoa(i)
					log("MultiHash start iter", strVal, strI)
					crc32 := DataSignerCrc32(strI + strVal)
					results[i] = crc32
					log("MultiHash done iter", strVal, strI, crc32)
				}(i, wg)
			}
			runtime.Gosched()
			wg.Wait()
			hash := strings.Join(results[:], ``)
			c <- hash
			log("MultiHash done", strVal)
		})
		out <- resultChan
		runtime.Gosched()
	}
}

// CombineResults получает все результаты, сортирует (https://golang.org/pkg/sort/), объединяет отсортированный
// результат через _ (символ подчеркивания) в одну строку

func CombineResults(in, out chan interface{}) {
	var values []string
	for val := range in {
		chanVal, ok := val.(chan string)
		if !ok {
			panic(fmt.Sprintf("value is not string channel '%s'", val))
		}
		strVal := <-chanVal
		//strVal, ok := val.(string)
		//if !ok {
		//	panic(fmt.Sprintf("value is not string '%s'", val))
		//}
		log("CombineResults item", strVal)
		values = append(values, strVal)
		runtime.Gosched()
	}
	log("CombineResults sort", len(values))
	sort.Strings(values)
	hash := strings.Join(values, `_`)
	out <- hash
}

func async(fn func(chan string)) chan string {
	c := make(chan string, 1)
	go func() {
		defer close(c)
		fn(c)
	}()
	return c
}

func logTime() string {
	now := time.Now()
	return fmt.Sprintf("%02d:%02d:%02d.%d", now.Hour(), now.Minute(), now.Second(), now.UnixMilli())
}

func log(args ...interface{}) {
	var withTime []interface{}
	withTime = append(withTime, logTime())
	withTime = append(withTime, args...)
	fmt.Println(withTime...)
}
