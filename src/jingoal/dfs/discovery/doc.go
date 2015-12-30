// Pacakge discovery processes biz of service register and discovery.
// Example for register a server:
//
// r := NewZKDfsServerRegister(
//     "10.1.0.20:2181,10.1.0.21:2181", 1*time.Second)
//
// s := DfsServer{Id: "myServer"}
//
// if err := r.Register(&s); err != nil {
// 	log.Printf("register error %v", err)
// }
//
package discovery
