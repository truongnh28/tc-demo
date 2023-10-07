package domain

type OrderRequest struct {
	UserId string `json:"user_id,omitempty"`
	BookId string `json:"book_id,omitempty"`
	Number uint   `json:"number,omitempty"`
}
