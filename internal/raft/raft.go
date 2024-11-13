// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"

	//"crypto/rand"
	"sync"
	"time"

	//"net/rpc"

	"raft/internal/comun/rpctimeout"
)

const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = false

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = false

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

type EntradaLogs struct {
	Operacion TipoOperacion
	Indice    int
	Mandato   int
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort
	Yo      int // indice de este nodo en campo array "nodos"
	IdLider int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	// Estado persistente en todos los nodos
	MandatoActual int
	NodoVotado    int
	logEntries    []EntradaLogs

	// Estado volátil en todos los nodos
	lastApplied int
	commitIndex int

	// Estado volátil en los líderes
	nextIndex  []int
	MatchIndex []int

	// Variables adicionales
	RolNodo            string
	VotosRecibidos     int
	LatidoLider        chan bool
	HayMandatoSuperior chan bool
	HayMayoria         chan bool
	AnyadirEntrada      chan EntradaLogs
}

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = -1

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		fmt.Println("nombreNodo: ", nombreNodo)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(
				fmt.Sprintf("%s/%s.txt", kLogOutputDir, nombreNodo),
				os.O_RDWR|os.O_CREATE|os.O_TRUNC,
				0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile,
				nombreNodo+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(io.Discard, "", 0)
	}

	// Añadir codigo de inicialización
	nr.RolNodo = "seguidor"
	nr.MandatoActual = 0
	nr.NodoVotado = IntNOINICIALIZADO
	nr.VotosRecibidos = 0
	nr.LatidoLider = make(chan bool)
	nr.HayMandatoSuperior = make(chan bool)
	nr.HayMayoria = make(chan bool)

	go maquinaEstados(nr)

	return nr
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int
	var esLider bool
	var idLider int = nr.IdLider

	// Vuestro codigo aqui
	mandato = nr.MandatoActual
	if yo == idLider {
		esLider = true
	} else {
		esLider = false
	}

	return yo, mandato, esLider, idLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantía que esta operación consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Resultado de este método :
// - Primer valor devuelto es el indice del registro donde se va a colocar
// - la operacion si consigue comprometerse.
// - El segundo valor es el mandato en curso
// - El tercer valor es true si el nodo cree ser el lider
// - Cuarto valor es el lider, es el indice del líder si no es él
// - Quinto valor es el resultado de aplicar esta operación en máquina de estados
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	indice := -1
	mandato := -1
	EsLider := false
	idLider := -1
	valorADevolver := ""

	fmt.Println(operacion)

	// VUESTRO CODIGO AQUI
	nr.Yo, mandato, EsLider, idLider = nr.obtenerEstado()

	nuevaEntrada := EntradaLogs{
		Operacion: operacion,
		Indice:    indice,
		Mandato:   mandato,
	}

	if EsLider {
		nuevaEntrada := EntradaLogs{
			Operacion: operacion,
			Indice:    indice,
			Mandato:   mandato,
		}
		nr.logEntries = append(nr.logEntries, nuevaEntrada)
	} else {

	}

	return indice, mandato, EsLider, idLider, valorADevolver
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type ArgsPeticionVoto struct {
	// Vuestros datos aqui
	IdCandidato int
	Mandato     int
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type RespuestaPeticionVoto struct {
	// Vuestros datos aqui
	CandidatoVotado bool
	Mandato         int
}

// Metodo para RPC PedirVoto
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	// Vuestro codigo aqui
	if (peticion.Mandato < nr.MandatoActual) ||
		(peticion.Mandato == nr.MandatoActual &&
			peticion.IdCandidato != nr.NodoVotado) {
		reply.CandidatoVotado = false
		reply.Mandato = nr.MandatoActual
	} else {
		nr.MandatoActual = peticion.Mandato
		nr.NodoVotado = peticion.IdCandidato
		reply.CandidatoVotado = true
		reply.Mandato = peticion.Mandato
		nr.HayMandatoSuperior <- true
	}
	return nil
}

type ArgAppendEntries struct {
	// Vuestros datos aqui
	Mandato int
	IdLider int
	Entrada EntradaLogs
}

type Results struct {
	// Vuestros datos aqui
	Mandato             int
	EntradaComprometida bool
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {
	// Completar....
	if args.Entrada == (EntradaLogs{}) {
		if args.Mandato < nr.MandatoActual {
			results.Mandato = nr.MandatoActual
		} else if args.Mandato == nr.MandatoActual {
			nr.IdLider = args.IdLider
			results.Mandato = nr.MandatoActual
			nr.LatidoLider <- true
		} else if args.Mandato > nr.MandatoActual {
			nr.IdLider = args.IdLider
			nr.MandatoActual = args.Mandato
			results.Mandato = args.Mandato
			if nr.RolNodo == "lider" {
				nr.HayMandatoSuperior <- true
			} else if nr.RolNodo == "candidato" {
				nr.HayMandatoSuperior <- true
				nr.LatidoLider <- true
			} else {
				nr.LatidoLider <- true
			}
		}
	}

	return nil
}

// --------------------------------------------------------------------------
// ----- METODOS/FUNCIONES desde nodo Raft, como cliente, a otro nodo Raft
// --------------------------------------------------------------------------

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros)
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timeout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre de todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	fmt.Println(nodo, args, reply)

	// Completar con la llamada RPC correcta incluida
	mayoriaVotos := len(nr.Nodos) / 2
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto", args,
		reply, 20*time.Millisecond)
	if err != nil {
		return false
	} else {
		if reply.Mandato > nr.MandatoActual {
			nr.MandatoActual = reply.Mandato
			nr.HayMandatoSuperior <- true
		} else if reply.CandidatoVotado {
			nr.VotosRecibidos += 1
			if nr.VotosRecibidos > mayoriaVotos {
				nr.HayMayoria <- true
			}
		}
		return true
	}

}

func (nr *NodoRaft) enviarLatidosAppendEntry(nodo int, args *ArgAppendEntries,
	results *Results) bool {
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries", args,
		results, 20*time.Millisecond)
	if err != nil {
		return false
	} else {
		if results.Mandato > nr.MandatoActual {
			nr.MandatoActual = results.Mandato
			nr.IdLider = -1
			nr.HayMandatoSuperior <- true
		}
		return true
	}
}

func maquinaEstados(nr *NodoRaft) {
	for {
		if nr.RolNodo == "seguidor" {
			gestionSeguidor(nr)
		}
		if nr.RolNodo == "candidato" {
			gestionCandidato(nr)
		}
		if nr.RolNodo == "lider" {
			gestionLider(nr)
		}
	}
}

func randomTimeout() time.Duration {
	minMilliseconds := big.NewInt(700)
	maxMilliseconds := big.NewInt(2000)

	randomMilliseconds, err := rand.Int(rand.Reader, maxMilliseconds)
	if err != nil {
		fmt.Println("Error al generar el valor aleatorio:", err)
	}

	randomMilliseconds.Add(randomMilliseconds, minMilliseconds)
	return time.Duration(randomMilliseconds.Int64()) * time.Millisecond
}

func gestionSeguidor(nr *NodoRaft) {
	timeOutLatido := randomTimeout()
	select {
	case <-nr.LatidoLider: //recibe latidos del lider
		fmt.Print("Recibido latido de lider")
	case <-time.After(timeOutLatido): //pasa el timeout de los latidos y comienza eleccion
		nr.RolNodo = "candidato"
		nr.IdLider = -1
	case <-nr.HayMandatoSuperior: //dimite y vuelve a ser seguidor
		nr.RolNodo = "seguidor"
	}
	case <- nr.Añadir
}

func gestionCandidato(nr *NodoRaft) {
	timeOutEleccion := randomTimeout()
	nr.MandatoActual += 1
	nr.NodoVotado = nr.Yo
	nr.VotosRecibidos += 1
	enviarVotosAux(nr)
	select {
	case <-nr.LatidoLider: //recibe latidos del lider
		fmt.Print("Recibido latido de lider, vuelve a ser seguidor")
		nr.RolNodo = "seguidor"
	case <-time.After(timeOutEleccion): //no se consigue mayoria
		fmt.Print("Hay que hacer reelecciones")
	case <-nr.HayMandatoSuperior: //dimite y vuelve a ser seguidor
		nr.RolNodo = "seguidor"
	case <-nr.HayMayoria: //hay mayoria y se convierte en lider
		fmt.Print("Este nodo es ahora lider")
		nr.RolNodo = "lider"
	}
}

func enviarVotosAux(nr *NodoRaft) {
	var respuesta RespuestaPeticionVoto
	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo {
			go nr.enviarPeticionVoto(i, &ArgsPeticionVoto{nr.Yo,
				nr.MandatoActual}, &respuesta)
		}
	}
}

func gestionLider(nr *NodoRaft) {
	nr.IdLider = nr.Yo
	latidoTimer := time.NewTimer(100 * time.Millisecond)
	enviarLatidosAux(nr)
	select {
	case <-nr.HayMandatoSuperior: //dimite y vuelve a ser seguidor
		nr.RolNodo = "seguidor"
	case <-latidoTimer.C:
		fmt.Print("Latido enviado a los seguidores")
	}
}

func enviarLatidosAux(nr *NodoRaft) {
	var results Results
	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo {
			go nr.enviarLatidosAppendEntry(i,
				&ArgAppendEntries{nr.MandatoActual, nr.IdLider,
					EntradaLogs{}}, &results)
		}
	}
}
